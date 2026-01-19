using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Collections;

namespace plugindatavarse
{
    public class Plugin_Validator : IPlugin
    {
        private readonly string _unsecureConfig;
        private readonly string _secureConfig;

        public Plugin_Validator(string unsecureConfig, string secureConfig)
        {
            _unsecureConfig = unsecureConfig;
            _secureConfig = secureConfig;
        }

        public void Execute(IServiceProvider serviceProvider)
        {
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            ITracingService tracing = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
            trace.Trace($"Start: ");
            if (string.IsNullOrWhiteSpace(_unsecureConfig)) return;

            // 1. Xác định Entity và ID hiện tại
            Entity entity = null;
            Guid currentId = Guid.Empty;
            
            // Lấy Target (Dữ liệu đang được gửi lên)
            if (context.InputParameters.Contains("Target"))
            {
                if (context.InputParameters["Target"] is Entity e)
                {
                    entity = e;
                    currentId = e.Id;
                }
                else if (context.InputParameters["Target"] is EntityReference er)
                {
                    currentId = er.Id; // Trường hợp Delete
                }
            }

            // Xử lý ID cho trường hợp Create (Output)
            if (currentId == Guid.Empty && context.OutputParameters.Contains("id"))
                currentId = (Guid)context.OutputParameters["id"];

            // 2. Chuẩn bị dữ liệu đầy đủ (Full Entity) để so sánh
            // Ưu tiên dùng PreImage nếu có (đặc biệt cho Delete hoặc Update)
            Entity fullEntity = new Entity(context.PrimaryEntityName) { Id = currentId };

            // Nếu có PreImage (Dữ liệu cũ), chép vào fullEntity
            if (context.PreEntityImages.Contains("PreImage") && context.PreEntityImages["PreImage"] is Entity img)
            {
                fullEntity = img;
            }
            // Nếu không có PreImage mà là Update/Delete, thử Retrieve (nếu cần thiết, cẩn thận performance)
            else if (context.MessageName.ToLower() != "create")
            {
                try
                {
                    fullEntity = service.Retrieve(context.PrimaryEntityName, currentId, new ColumnSet(true));
                }
                catch { /* Bỏ qua nếu không retrieve được (vd Create Pre-op) */ }
            }

            // Nếu là Create/Update, merge dữ liệu mới nhất từ Target vào fullEntity
            if (entity != null)
            {
                foreach (var attr in entity.Attributes) fullEntity[attr.Key] = attr.Value;
            }

            // --- 3. XỬ LÝ CẤU HÌNH (CONFIG) ---
            string configRules = _unsecureConfig;
            string saveToColumn = null;

            // Kiểm tra xem có yêu cầu lưu lỗi vào cột không (SAVE_TO:)
            if (configRules.Trim().StartsWith("SAVE_TO:", StringComparison.OrdinalIgnoreCase))
            {
                int firstPipe = configRules.IndexOf('|');
                if (firstPipe > 0)
                {
                    string header = configRules.Substring(0, firstPipe);
                    saveToColumn = header.Split(':')[1].Trim();
                    configRules = configRules.Substring(firstPipe + 1); // Cắt bỏ phần header
                }
            }

            List<string> errorMessages = new List<string>();

            try
            {
                string[] rules = configRules.Split('|');

                foreach (string rule in rules)
                {
                    
                    if (string.IsNullOrWhiteSpace(rule)) continue;
                    string processingRule = rule.Trim();
                    // Xử lý Điều kiện tiên quyết (=>)
                    // VD: Status != Won => Check...
                    if (processingRule.Contains("=>"))
                    {
                        var parts = processingRule.Split(new string[] { "=>" }, StringSplitOptions.None);
                        if (!EvaluateExpression(service, fullEntity, currentId, parts[0].Trim(), tracing)) continue;
                        processingRule = parts[1].Trim();
                    }

                    // Xử lý Validate (Message : Condition)
                    var msgParts = processingRule.Split(':');
                    if (msgParts.Length < 2) continue;

                    string message = msgParts[0].Trim();
                    string condition = msgParts[1].Trim();
                    if (EvaluateExpression(service, fullEntity, currentId, condition, tracing))
                    {
                        errorMessages.Add(message);
                    }
                }

                // --- 4. KẾT QUẢ ---

                // Mode 1: Ghi vào cột (Không chặn lưu)
                if (!string.IsNullOrEmpty(saveToColumn) && entity != null)
                {
                    
                    // Chỉ ghi được nếu có Target (Create/Update). Delete không ghi được.
                    if (errorMessages.Count > 0) 
                    {
                        string finalMessage = string.Join(", ", errorMessages);

                        // --- CODE MỚI: CHỐNG TRÀN KÝ TỰ (TRUNCATE SAFETY) ---
                        // Giả sử cột Text Area trong Dataverse tối đa 2000-4000 ký tự.
                        // Ta giới hạn cứng ở 2000 để an toàn cho mọi loại cột.
                        int maxLength = 3000;
                        trace.Trace($"Start: {finalMessage.Length}");
                        if (finalMessage.Length > maxLength)
                        {
                            
                            finalMessage = finalMessage.Substring(0, maxLength - 5) + "...";
                        }
                        // ----------------------------------------------------
                        entity[saveToColumn] = finalMessage;
                    }   
                    else
                        entity[saveToColumn] = null; // Xóa trắng nếu hết lỗi
                }
                // Mode 2: Bắn lỗi (Chặn lưu)
                else if (errorMessages.Count > 0)
                {
                    throw new InvalidPluginExecutionException(string.Join("\n", errorMessages));
                }
            }
            catch (InvalidPluginExecutionException) { throw; }
            catch (Exception ex)
            {
                tracing.Trace(ex.ToString());
                // Nếu đang dùng mode SAVE_TO thì ghi lỗi hệ thống vào cột luôn
                if (!string.IsNullOrEmpty(saveToColumn) && entity != null)
                    entity[saveToColumn] = "System Error: " + ex.Message;
                else
                    throw new InvalidPluginExecutionException($"Validator Error: {ex.Message}");
            }
        }

        // Hàm đệ quy kiểm tra điều kiện (hỗ trợ AND &)
        private bool EvaluateExpression(IOrganizationService s, Entity e, Guid id, string logic, ITracingService t)
        {
            var conditions = logic.Split('&');
            foreach (var cond in conditions)
            {
                if (!CheckCondition(s, e, id, cond.Trim(), t)) return false;
            }
            return true;
        }

        // Hàm kiểm tra chi tiết từng điều kiện
        private bool CheckCondition(IOrganizationService s, Entity e, Guid id, string c, ITracingService t)
        {
            try
            {
                // 1. Xác định toán tử
                string op = "=";
                if (c.Contains(">=")) op = ">=";
                else if (c.Contains("<=")) op = "<=";
                else if (c.Contains("!=")) op = "!=";
                else if (c.Contains("!%")) op = "!%";
                else if (c.Contains("%")) op = "%";
                else if (c.Contains(">")) op = ">";
                else if (c.Contains("<")) op = "<";

                var parts = c.Split(new string[] { op }, StringSplitOptions.None);
                string leftSide = parts[0].Trim();
                string rightSide = parts[1].Trim();

                // 2. Lấy giá trị vế trái (Left Value)
                object leftVal = null;
                bool isNum = false;

                if (leftSide.StartsWith("COUNT_PARENT", StringComparison.OrdinalIgnoreCase))
                {
                    leftVal = CountParent(s, e, leftSide);
                    isNum = true;
                }
                else if (leftSide.StartsWith("COUNT", StringComparison.OrdinalIgnoreCase))
                {
                    leftVal = CountNormal(s, id, leftSide);
                    isNum = true;
                }
                else
                {
                    leftVal = GetValue(e, leftSide);
                }

                // 3. Lấy giá trị vế phải (Right Value)
                object rightVal = null;

                // Case: So với trường khác
                if (rightSide.StartsWith("FIELD:", StringComparison.OrdinalIgnoreCase))
                {
                    string f = rightSide.Substring(6).Trim();
                    int offset = 0;
                    if (f.Contains("+")) { var p = f.Split('+'); f = p[0].Trim(); offset = int.Parse(p[1]); }
                    else if (f.Contains("-")) { var p = f.Split('-'); f = p[0].Trim(); offset = -int.Parse(p[1]); }

                    rightVal = GetValue(e, f);
                    if (rightVal is DateTime dt) rightVal = dt.AddDays(offset);
                }
                // Case: So với ngày hôm nay
                else if (rightSide.ToLower().Contains("today"))
                {
                    int offset = 0;
                    if (rightSide.Contains("+")) offset = int.Parse(rightSide.Split('+')[1]);
                    else if (rightSide.Contains("-")) offset = -int.Parse(rightSide.Split('-')[1]);
                    rightVal = DateTime.UtcNow.Date.AddDays(offset);
                }
                // Case: Giá trị tĩnh
                else
                {
                    rightVal = rightSide.ToLower();
                }

                // 4. CHUYỂN ĐỔI SANG CHUỖI ĐỂ SO SÁNH (FIX LỖI)
                string s1 = GetString(leftVal);
                string s2 = GetString(rightVal);

                // Log để debug (nếu cần)
                // t.Trace($"Compare Final: '{s1}' {op} '{s2}'");

                // --- A. SO SÁNH BẰNG / KHÁC / CHỨA ---
                // Logic mới: So sánh chuỗi trực tiếp. "null" == "null" sẽ là True.
                if (op == "=") return s1 == s2;
                if (op == "!=") return s1 != s2;
                if (op == "%") return s1.Contains(s2);
                if (op == "!%") return !s1.Contains(s2);

                // --- B. SO SÁNH LỚN / BÉ (>, <, >=, <=) ---

                // B1. Nếu gặp NULL ở bất kỳ vế nào -> Trả về FALSE ngay (Không so sánh được)
                // Điều này ngăn chặn lỗi "Ngày < Today" báo True khi ngày đang trống.
                if (s1 == "null" || s2 == "null") return false;

                // B2. So sánh SỐ (Nếu cả 2 chuỗi convert được sang số)
                if (decimal.TryParse(s1, out decimal n1) && decimal.TryParse(s2, out decimal n2))
                {
                    switch (op) { case ">": return n1 > n2; case "<": return n1 < n2; case ">=": return n1 >= n2; case "<=": return n1 <= n2; }
                }

                // B3. So sánh NGÀY / CHUỖI (Dùng hàm Compare có sẵn của String)
                // Định dạng yyyy-MM-dd cho phép so sánh chuỗi tương đương so sánh ngày
                int comp = string.Compare(s1, s2);
                switch (op) { case ">": return comp > 0; case "<": return comp < 0; case ">=": return comp >= 0; case "<=": return comp <= 0; }

                return false;
            }
            catch (Exception ex)
            {
                t.Trace($"Check Error '{c}': {ex.Message}");
                return false;
            }
        }

        // Helper lấy giá trị từ Entity
        private object GetValue(Entity e, string f)
        {
            return e.Contains(f) ? e[f] : null;
        }

        // Helper chuyển mọi thứ về string chữ thường
        private string GetString(object o)
        {
            if (o == null) return "null";

            // Xử lý Lookup
            if (o is EntityReference er) return er.Name?.ToLower() ?? er.Id.ToString();

            // Xử lý Single OptionSet (Chọn 1)
            if (o is OptionSetValue os) return os.Value.ToString();

            // --- FIX LỖI: Xử lý Multi-Select OptionSet (Chọn nhiều) ---
            // Vì DLL cũ không có class OptionSetValueCollection, ta kiểm tra bằng tên class
            if (o.GetType().Name == "OptionSetValueCollection")
            {
                List<string> values = new List<string>();
                // Ép kiểu về danh sách chung để duyệt
                foreach (var item in (IEnumerable)o)
                {
                    if (item is OptionSetValue itemOsv)
                    {
                        values.Add(itemOsv.Value.ToString());
                    }
                }
                return string.Join(",", values);
            }
            // -----------------------------------------------------------

            if (o is bool b) return b.ToString().ToLower();
            if (o is DateTime dt) return dt.ToString("yyyy-MM-dd");

            return o.ToString().ToLower();
        }

        // Đếm bảng con dựa trên ID hiện tại (VD: Account đếm Contact con)
        private int CountNormal(IOrganizationService s, Guid id, string f)
        {
            var m = Regex.Match(f, @"COUNT\((.*?)\)");
            if (!m.Success || id == Guid.Empty) return 0;
            var args = m.Groups[1].Value.Split(',');
            QueryExpression q = new QueryExpression(args[0].Trim()) { ColumnSet = new ColumnSet(false) };
            q.Criteria.AddCondition(args[1].Trim(), ConditionOperator.Equal, id);
            if (args.Length >= 4) q.Criteria.AddCondition(args[2].Trim(), ConditionOperator.Equal, args[3].Trim());
            return s.RetrieveMultiple(q).Entities.Count;
        }

        // Đếm bảng khác dựa trên ID lấy từ 1 trường Lookup (VD: Contact đếm Contact anh em cùng Account cha)
        private int CountParent(IOrganizationService s, Entity e, string f)
        {
            var m = Regex.Match(f, @"COUNT_PARENT\((.*?)\)");
            if (!m.Success) return 0;
            var args = m.Groups[1].Value.Split(',');
            string myLookup = args[0].Trim();
            string targetTable = args[1].Trim();
            string targetLookup = args[2].Trim();

            if (!e.Contains(myLookup) || e[myLookup] == null) return 0;
            Guid parentId = ((EntityReference)e[myLookup]).Id;

            QueryExpression q = new QueryExpression(targetTable) { ColumnSet = new ColumnSet(false) };
            q.Criteria.AddCondition(targetLookup, ConditionOperator.Equal, parentId);
            return s.RetrieveMultiple(q).Entities.Count;
        }
    }
}