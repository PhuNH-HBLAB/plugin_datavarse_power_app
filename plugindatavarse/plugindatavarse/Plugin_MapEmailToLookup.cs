using System;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace plugindatavarse
{
    public class Plugin_MapEmailToLookup : IPlugin
    {
        private readonly string _unsecureConfig;
        private readonly string _secureConfig;

        public Plugin_MapEmailToLookup(string unsecureConfig, string secureConfig)
        {
            _unsecureConfig = unsecureConfig;
            _secureConfig = secureConfig;
        }

        public void Execute(IServiceProvider serviceProvider)
        {
            // 1. Khởi tạo dịch vụ
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(null);
            trace.Trace($"Start: ");
            trace.Trace($"context.Depth: {context.Depth}");
            // 2. Kiểm tra điều kiện cơ bản
            //if (context.Depth > 1) return; // Tránh lặp vô tận
            if (string.IsNullOrWhiteSpace(_unsecureConfig))
            {
                trace.Trace("Config is empty -> Stop.");
                return;
            }
            trace.Trace($"Start: {context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity}");
            // 3. Xử lý chính
            if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity)
            {
                Entity entity = (Entity)context.InputParameters["Target"];

                try
                {
                    // [DEBUG] In ra ID để biết đang xử lý bản ghi nào
                    trace.Trace($"--- START PLUGIN: {entity.LogicalName} (ID: {entity.Id}) ---");

                    string[] listConfigs = _unsecureConfig.Split('|');

                    foreach (string singleConfig in listConfigs)
                    {
                        if (string.IsNullOrWhiteSpace(singleConfig)) continue;

                        // Parse Config: mc_account_bd_temp, cr987_account_bd, systemuser
                        string[] configParts = singleConfig.Split(',');
                        if (configParts.Length < 3) continue;

                        string fieldImportInput = configParts[0].Trim(); // Cột Temp
                        string fieldLookup = configParts[1].Trim();      // Cột Lookup
                        string tableLookupTo = configParts[2].Trim();    // Bảng User

                        // --- CHIẾN THUẬT LẤY DỮ LIỆU THÔNG MINH ---
                        string dataToProcess = "";
                        bool sourceFromTarget = false;

                        // Cách 1: Ưu tiên lấy từ Target (Dữ liệu mới nhất Excel vừa gửi lên)
                        if (entity.Contains(fieldImportInput) && entity[fieldImportInput] != null)
                        {
                            dataToProcess = entity[fieldImportInput].ToString();
                            sourceFromTarget = true;
                            trace.Trace($"[Source: Target] Lấy được từ Excel cột '{fieldImportInput}': {dataToProcess}");
                        }
                        // Cách 2: Nếu Target không có, dùng ID để lôi từ Database ra
                        // (Phòng trường hợp Excel coi là trùng lặp nên không gửi lên)
                        else
                        {
                            trace.Trace($"[Source: Database] Target thiếu cột '{fieldImportInput}'. Đang truy vấn DB...");
                            try
                            {
                                ColumnSet cols = new ColumnSet(fieldImportInput);
                                Entity fullRecord = service.Retrieve(entity.LogicalName, entity.Id, cols);

                                if (fullRecord.Contains(fieldImportInput) && fullRecord[fieldImportInput] != null)
                                {
                                    dataToProcess = fullRecord[fieldImportInput].ToString();
                                    trace.Trace($"[Source: Database] Tìm thấy trong DB: {dataToProcess}");
                                }
                                else
                                {
                                    trace.Trace($"[Source: Database] Trong DB cũng trống -> Bỏ qua.");
                                }
                            }
                            catch (Exception dbEx)
                            {
                                trace.Trace($"[Lỗi DB Retrieve]: {dbEx.Message}");
                            }
                        }

                        // --- XỬ LÝ DỮ LIỆU ---
                        if (string.IsNullOrWhiteSpace(dataToProcess)) continue;

                        // [Safety Check] Bỏ qua nếu dữ liệu bị lỗi Boolean (False/True) do code cũ
                        if (dataToProcess.Equals("False", StringComparison.OrdinalIgnoreCase) ||
                            dataToProcess.Equals("True", StringComparison.OrdinalIgnoreCase))
                        {
                            trace.Trace("!!! PHÁT HIỆN DỮ LIỆU LỖI BOOLEAN -> BỎ QUA !!!");
                            continue;
                        }

                        // [Safety Check] Bỏ qua nếu dữ liệu là "(Not Found)" để tránh lặp
                        if (dataToProcess.Contains("(Not Found)")) continue;

                        // 1. Làm sạch dữ liệu (Clean Data)
                        string cleanInput = dataToProcess.Replace("\u00A0", " ").Replace("\n", "").Replace("\r", "").Trim();
                        trace.Trace($"[Processing] Tìm kiếm User: '{cleanInput}'");

                        // 2. Query tìm kiếm User
                        QueryExpression query = new QueryExpression(tableLookupTo);
                        query.ColumnSet = new ColumnSet("firstname", "lastname", "fullname");
                        query.TopCount = 5;
                        query.Criteria.AddCondition("isdisabled", ConditionOperator.Equal, false); // Chỉ tìm user đang hoạt động

                        FilterExpression filter = new FilterExpression(LogicalOperator.Or);
                        filter.AddCondition("firstname", ConditionOperator.Equal, cleanInput);
                        filter.AddCondition("lastname", ConditionOperator.Equal, cleanInput);
                        filter.AddCondition("fullname", ConditionOperator.Like, $"%{cleanInput}%");

                        query.Criteria.AddFilter(filter);

                        EntityCollection results = service.RetrieveMultiple(query);
                        Guid foundId = Guid.Empty;
                        bool isMatch = false;

                        // 3. Logic so khớp kết quả
                        if (results.Entities.Count > 0)
                        {
                            foreach (var user in results.Entities)
                            {
                                string fName = (user.GetAttributeValue<string>("firstname") ?? "").Trim();
                                string lName = (user.GetAttributeValue<string>("lastname") ?? "").Trim();
                                string fullName = (user.GetAttributeValue<string>("fullname") ?? "").Trim();

                                // So khớp chính xác Tên hoặc Họ
                                bool matchName = fName.Equals(cleanInput, StringComparison.OrdinalIgnoreCase) ||
                                                 lName.Equals(cleanInput, StringComparison.OrdinalIgnoreCase);

                                // So khớp Fullname (có chứa)
                                bool matchFull = fullName.IndexOf(cleanInput, StringComparison.OrdinalIgnoreCase) >= 0;

                                // So khớp đặc biệt (Tách dấu #)
                                bool matchSplit = false;
                                if (!string.IsNullOrEmpty(fullName))
                                {
                                    string[] parts = fullName.Split('#');
                                    matchSplit = parts.Any(p => p.Trim().Equals(cleanInput, StringComparison.OrdinalIgnoreCase));
                                }

                                if (matchName || matchFull || matchSplit)
                                {
                                    foundId = user.Id;
                                    isMatch = true;
                                    trace.Trace($"-> MATCHED USER: {fullName}");
                                    break;
                                }
                            }
                        }

                        // 4. Update kết quả vào Target (Để lưu xuống DB)
                        if (isMatch)
                        {
                            // Gán Lookup
                            entity[fieldLookup] = new EntityReference(tableLookupTo, foundId);
                            // Xóa trắng ô Temp
                            entity[fieldImportInput] = null;
                            trace.Trace("Update thành công: Đã gán Lookup & Xóa Temp.");
                        }
                        else
                        {
                            // Báo lỗi vào ô Temp (Chỉ update nếu dữ liệu chưa có chữ Not Found)
                            string errorMsg = $"{cleanInput} (Not Found)";
                            if (!dataToProcess.Equals(errorMsg))
                            {
                                entity[fieldImportInput] = errorMsg;
                                trace.Trace("Không tìm thấy User -> Ghi chú Not Found.");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    trace.Trace($"CRITICAL ERROR: {ex.ToString()}");
                    throw new InvalidPluginExecutionException($"Plugin Error: {ex.Message}");
                }
            }
        }
    }
}