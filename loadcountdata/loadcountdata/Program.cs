using System;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector; // Thư viện kết nối CRM

namespace FixDataCountTool
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== TOOL CẬP NHẬT SỐ LƯỢNG CON (CONTACT) CHO CHA (ACCOUNT) ===");

            // 1. CẤU HÌNH KẾT NỐI
            // Bạn thay thông tin của bạn vào dòng bên dưới
            // UserName: Tài khoản đăng nhập CRM
            // Password: Mật khẩu
            // Url: Địa chỉ môi trường (Ví dụ: https://org12345.crm5.dynamics.com)

            string connectionString = @"
                AuthType=OAuth;
                Url=https://org4536e97b.crm5.dynamics.com;
                UserName=phunh@hblabvn.onmicrosoft.com;
                Password=Phu001204036687@;
                AppId=51f81489-12ee-4a9e-aaae-a2591f45987d;
                RedirectUri=app://58145B91-0C36-4500-8554-080854F2AC97;";

            //string connectionString = @"
            //    AuthType=OAuth;
            //    Url=https://org3c659c24.crm5.dynamics.com;
            //    UserName=phunh@hblabvn.onmicrosoft.com;
            //    Password=Phu001204036687@;
            //    AppId=51f81489-12ee-4a9e-aaae-a2591f45987d;
            //    RedirectUri=app://58145B91-0C36-4500-8554-080854F2AC97;";

            // đếm số lượng contact

            //    try
            //    {
            //        Console.WriteLine("Đang kết nối đến Dataverse...");
            //        CrmServiceClient service = new CrmServiceClient(connectionString);

            //        if (!service.IsReady)
            //        {
            //            Console.WriteLine("Lỗi kết nối: " + service.LastCrmError);
            //            Console.ReadLine();
            //            return;
            //        }
            //        Console.WriteLine("Kết nối thành công! Đang lấy danh sách Account...");

            //        // 2. CẤU HÌNH TÊN BẢNG VÀ TRƯỜNG (Check kỹ đoạn này)
            //        string parentTable = "hbl_account";           // Bảng Cha
            //        string parentIdField = "hbl_accountid";       // ID bảng Cha
            //        string countField = "hbl_account_total_contacts"; // Trường lưu số lượng trên Cha

            //        string childTable = "hbl_contact";            // Bảng Con
            //        string childIdField = "hbl_contactid";        // ID bảng Con
            //        string lookupField = "hbl_contact_account";   // Lookup từ Con trỏ về Cha

            //        // 3. LẤY TẤT CẢ ACCOUNT CHA
            //        // (Lấy tối đa 5000 bản ghi - nếu nhiều hơn cần phân trang, nhưng tool chạy 1 lần thế này là ổn)
            //        QueryExpression query = new QueryExpression(parentTable);
            //        query.ColumnSet = new ColumnSet(parentIdField); // Chỉ cần lấy ID để tối ưu tốc độ

            //        // (Tùy chọn) Nếu chỉ muốn chạy cho Account đang hoạt động thì mở comment dòng dưới
            //        // query.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);

            //        EntityCollection parents = service.RetrieveMultiple(query);
            //        Console.WriteLine($"=> Tìm thấy {parents.Entities.Count} Account. Bắt đầu tính toán...");

            //        // 4. DUYỆT QUA TỪNG CHA ĐỂ TÍNH LẠI
            //        int processed = 0;
            //        foreach (var parent in parents.Entities)
            //        {
            //            Guid parentId = parent.Id;

            //            // Dùng FetchXML để đếm số lượng con (Nhanh hơn lấy hết con về đếm)
            //            string fetchXml = $@"
            //            <fetch distinct='false' mapping='logical' aggregate='true'>
            //                <entity name='{childTable}'>
            //                    <attribute name='{childIdField}' alias='cnt' aggregate='count'/>
            //                    <filter>
            //                        <condition attribute='{lookupField}' operator='eq' value='{parentId}' />
            //                    </filter>
            //                </entity>
            //            </fetch>";

            //            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));

            //            int count = 0;
            //            if (result.Entities.Count > 0 && result.Entities[0].Contains("cnt"))
            //            {
            //                count = (int)((AliasedValue)result.Entities[0]["cnt"]).Value;
            //            }

            //            // Update lại vào Cha
            //            Entity parentUpdate = new Entity(parentTable, parentId);
            //            parentUpdate[countField] = count;
            //            service.Update(parentUpdate);

            //            processed++;
            //            // In tiến độ ra màn hình cho đỡ sốt ruột
            //            Console.WriteLine($"[{processed}/{parents.Entities.Count}] Updated ID {parentId} => Count: {count}");
            //        }

            //        Console.WriteLine("=== HOÀN TẤT CẬP NHẬT DỮ LIỆU ===");
            //    }
            //    catch (Exception ex)
            //    {
            //        Console.WriteLine("Xảy ra lỗi: " + ex.Message);
            //    }

            //    Console.WriteLine("Bấm phím bất kỳ để thoát...");
            //    Console.ReadLine();
            //}

            // chuyển đổi data bảng account

            //try
            //{
            //    CrmServiceClient service = new CrmServiceClient(connectionString);
            //    if (!service.IsReady)
            //    {
            //        Console.WriteLine("Lỗi kết nối: " + service.LastCrmError);
            //        return;
            //    }
            //    Console.WriteLine("Kết nối thành công! Đang xử lý...");

            //    string tableName = "hbl_contact";

            //    // 2. KHAI BÁO 3 CẶP (NGUỒN -> ĐÍCH)
            //    // Cặp 1
            //    string src1 = "hbl_contact_summary_working_history";
            //    string tgt1 = "mc_contact_summary_working_history";

            //    // Cặp 2
            //    //string src2 = "hbl_account_list_opps_old";
            //    //string tgt2 = "mc_account_list_opps_old";

            //    //// Cặp 3
            //    //string src3 = "hbl_account_investigated_info";
            //    //string tgt3 = "mc_account_investigated_info";

            //    // 3. LẤY DỮ LIỆU
            //    QueryExpression query = new QueryExpression(tableName);

            //    // Chỉ lấy 3 cột nguồn
            //    query.ColumnSet = new ColumnSet(src1);

            //    // Lọc: Chỉ lấy bản ghi mà ÍT NHẤT 1 trong 3 cột nguồn có dữ liệu
            //    // Để tránh tải về các account trống trơn gây chậm tool
            //    FilterExpression filter = new FilterExpression(LogicalOperator.Or);
            //    filter.AddCondition(src1, ConditionOperator.NotNull);
            //    //filter.AddCondition(src2, ConditionOperator.NotNull);
            //    //filter.AddCondition(src3, ConditionOperator.NotNull);
            //    query.Criteria.AddFilter(filter);

            //    // Nếu cần lọc thêm (ví dụ chỉ Active Account) thì uncomment dòng dưới:
            //    // query.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);

            //    EntityCollection accounts = service.RetrieveMultiple(query);
            //    Console.WriteLine($"=> Tìm thấy {accounts.Entities.Count} bản ghi có dữ liệu nguồn.");

            //    int processed = 0;

            //    // 4. DUYỆT VÀ UPDATE
            //    foreach (var acc in accounts.Entities)
            //    {
            //        Entity updateEntity = new Entity(tableName, acc.Id);
            //        bool needUpdate = false;

            //        // --- XỬ LÝ CẶP 1 ---
            //        if (acc.Contains(src1)) // Kiểm tra cột có tồn tại trong kết quả trả về
            //        {
            //            string val1 = acc.GetAttributeValue<string>(src1);
            //            if (!string.IsNullOrEmpty(val1))
            //            {
            //                updateEntity[tgt1] = val1;
            //                needUpdate = true;
            //            }
            //        }

            //        // --- XỬ LÝ CẶP 2 ---
            //        //if (acc.Contains(src2))
            //        //{
            //        //    string val2 = acc.GetAttributeValue<string>(src2);
            //        //    if (!string.IsNullOrEmpty(val2))
            //        //    {
            //        //        updateEntity[tgt2] = val2;
            //        //        needUpdate = true;
            //        //    }
            //        //}

            //        //// --- XỬ LÝ CẶP 3 ---
            //        //if (acc.Contains(src3))
            //        //{
            //        //    string val3 = acc.GetAttributeValue<string>(src3);
            //        //    if (!string.IsNullOrEmpty(val3))
            //        //    {
            //        //        updateEntity[tgt3] = val3;
            //        //        needUpdate = true;
            //        //    }
            //        //}

            //        // --- THỰC THI ---
            //        if (needUpdate)
            //        {
            //            service.Update(updateEntity);
            //            processed++;
            //            Console.WriteLine($"[{processed}] Updated Account ID: {acc.Id}");
            //        }
            //    }

            //    Console.WriteLine($"=== HOÀN TẤT: Đã cập nhật {processed} bản ghi ===");
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Lỗi: " + ex.Message);
            //}
            //Console.ReadLine();

            //try
            //{
            //    CrmServiceClient service = new CrmServiceClient(connectionString);
            //    if (!service.IsReady)
            //    {
            //        Console.WriteLine("Lỗi kết nối: " + service.LastCrmError);
            //        return;
            //    }
            //    Console.WriteLine("Kết nối thành công! Đang quét dữ liệu hợp đồng...");

            //    string tableName = "hbl_contract"; // Tên bảng hợp đồng của bạn
            //    string targetField = "mc_contract_meeting_category";

            //    // Khai báo tên Logical Name của các cột điều kiện
            //    string colEndDate = "hbl_contract_end_date";
            //    string colStatus = "hbl_contract_status";

            //    // Khai báo Value của Choice (Cần khớp với hệ thống của bạn)
            //    int statusNormal = 135150001;
            //    int statusFinished = 135150006;

            //    // 1. LẤY DỮ LIỆU CẦN THIẾT
            //    QueryExpression query = new QueryExpression(tableName);
            //    query.ColumnSet = new ColumnSet(colEndDate, colStatus); 

            //    EntityCollection contracts = service.RetrieveMultiple(query);
            //    Console.WriteLine($"=> Tìm thấy {contracts.Entities.Count} bản ghi cần kiểm tra.");

            //    int processed = 0;
            //    DateTime today = DateTime.Today;

            //    foreach (var con in contracts.Entities)
            //    {
            //        string resultText = string.Empty;

            //        DateTime? endDate = con.Contains(colEndDate) ? con.GetAttributeValue<DateTime?>(colEndDate) : null;
            //        OptionSetValue status = con.Contains(colStatus) ? con.GetAttributeValue<OptionSetValue>(colStatus) : null;

            //        if (endDate.HasValue)
            //        {
            //            int daysDiff = (endDate.Value.Date - today).Days;


            //            if (daysDiff >= 0 && daysDiff <= 30)
            //            {
            //                resultText = "Ending soon";
            //            }
            //            else if (status != null && status.Value != statusNormal && status.Value != statusFinished)
            //            {
            //                resultText = "Status check";
            //            }
            //            else if (status != null && status.Value == statusNormal && daysDiff < 0)
            //            {
            //                resultText = "Need Update Contract";
            //            }
            //        }


            //        Entity updateEntity = new Entity(tableName, con.Id);
            //        updateEntity[targetField] = resultText;

            //        service.Update(updateEntity);
            //        processed++;

            //        if (processed % 10 == 0) // Hiển thị tiến độ mỗi 10 bản ghi
            //            Console.WriteLine($"--- Đã xử lý {processed}/{contracts.Entities.Count} ---");
            //    }

            //    Console.WriteLine($"\n=== HOÀN TẤT: Đã cập nhật xong {processed} hợp đồng ===");
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Lỗi thực thi: " + ex.Message);
            //}
            //Console.ReadLine();

            //try
            //{
            //    CrmServiceClient service = new CrmServiceClient(connectionString);
            //    if (!service.IsReady)
            //    {
            //        Console.WriteLine("Lỗi kết nối: " + service.LastCrmError);
            //        return;
            //    }
            //    Console.WriteLine("Kết nối thành công! Bắt đầu sao chép dữ liệu...");

            //    // Tên bảng
            //    string sourceTable = "hbl_account_planning";
            //    string targetTable = "mc_account_actual";

            //    // 1. LẤY TOÀN BỘ DỮ LIỆU TỪ BẢNG A
            //    QueryExpression query = new QueryExpression(sourceTable);
            //    query.ColumnSet = new ColumnSet(true); // Lấy tất cả các cột để đảm bảo không sót trường nào trong ảnh

            //    EntityCollection sourceRecords = service.RetrieveMultiple(query);
            //    Console.WriteLine($"=> Tìm thấy {sourceRecords.Entities.Count} bản ghi từ {sourceTable}.");

            //    int processed = 0;

            //    foreach (var sourceEnt in sourceRecords.Entities)
            //    {
            //        Entity targetEnt = new Entity(targetTable);

            //        // --- MAPPING CÁC TRƯỜNG THEO ẢNH ---

            //        // Các trường text/số thông thường
            //        CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_market", "mc_account_actual_market");
            //        CopyAttribute(sourceEnt, targetEnt, "hblab_account_planning_account", "mc_actual_account");
            //        CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_resouce_type", "mc_account_actual_resouce_type");
            //        CopyAttribute(sourceEnt, targetEnt, "hbl_account_plan_unit_price", "mc_account_actual_unit_price");

            //        // Mapping vòng lặp cho mm1 -> mm12
            //        for (int i = 1; i <= 12; i++)
            //        {
            //            CopyAttribute(sourceEnt, targetEnt, $"hbl_mm{i}", $"mc_mm{i}");
            //        }

            //        CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_opportunity", "mc_account_actual_opportunity");
            //        CopyAttribute(sourceEnt, targetEnt, "hbl_account_planning_certainty", "mc_account_actual_certainty");
            //        CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_service_offering", "mc_account_actual_service_offering");
            //        CopyAttribute(sourceEnt, targetEnt, "hbl_account_planning_currency", "mc_account_actual_currency");
            //        CopyAttribute(sourceEnt, targetEnt, "hbl_link_acc_plan", "mc_account_actual_link_accplan"); 
            //        CopyAttribute(sourceEnt, targetEnt, "hbl_account_planning_year", "mc_year"); 
            //        CopyAttribute(sourceEnt, targetEnt, "hbl_newcolumn", "mc_account_actual_name");
            //        CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_ap_type", "mc_account_actual_ap_type");

            //        // 2. TẠO MỚI BẢN GHI Ở BẢNG B
            //        service.Create(targetEnt);

            //        processed++;
            //        if (processed % 10 == 0)
            //            Console.WriteLine($"--- Đã sao chép {processed}/{sourceRecords.Entities.Count} bản ghi ---");
            //    }

            //    Console.WriteLine($"\n=== HOÀN TẤT: Đã tạo {processed} bản ghi mới ở bảng {targetTable} ===");
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Lỗi thực thi: " + ex.Message);
            //}
            //Console.ReadLine();

            //// Hàm phụ trợ để kiểm tra dữ liệu trước khi copy (tránh lỗi null)
            //void CopyAttribute(Entity source, Entity target, string sourceAttr, string targetAttr)
            //{
            //    if (source.Contains(sourceAttr))
            //    {
            //        target[targetAttr] = source[sourceAttr];
            //    }
            //}


            try
            {
                CrmServiceClient service = new CrmServiceClient(connectionString);
                if (!service.IsReady)
                {
                    Console.WriteLine("Lỗi kết nối: " + service.LastCrmError);
                    return;
                }

                string tableName = "hbl_opportunities";
                string sourceField = "hbl_opportunities_win_rate"; // Trường chứa số (x)
                string targetField = "mc_opportunities_certainty"; // Trường Choice (b)

                // 1. Lấy tất cả bản ghi có giá trị Win Rate
                QueryExpression query = new QueryExpression(tableName);
                query.ColumnSet = new ColumnSet(sourceField);
                query.Criteria.AddCondition(sourceField, ConditionOperator.NotNull);

                EntityCollection records = service.RetrieveMultiple(query);
                Console.WriteLine($"=> Tìm thấy {records.Entities.Count} bản ghi cần xử lý.");

                int count = 0;
                foreach (var entity in records.Entities)
                {
                    // Lấy giá trị x
                    double x = 0;
                    var rawValue = entity[sourceField];

                    // Ép kiểu an toàn tùy theo định dạng Decimal hay Double
                    x = Convert.ToDouble(rawValue);

                    int? b = null;

                    // 2. Logic phân loại (Sửa lại toán tử && để đúng khoảng giá trị)
                    if (x >= 0 && x < 0.3)
                        b = 758130005; // IDX 0.1
                    else if (x >= 0.3 && x < 0.5)
                        b = 758130004; // Bestcase 0.3
                    else if (x >= 0.5 && x < 0.8)
                        b = 758130003; // Forecast 0.5
                    else if (x >= 0.8 && x < 0.9)
                        b = 758130002; // Extend 0.8
                    else if (x >= 0.9 && x < 1.0)
                        b = 758130001; // Worstcase 0.9
                    else if (x >= 1.0)
                        b = 758130000; // Contracted 1

                    if (b.HasValue)
                    {
                        // 3. Cập nhật lại chính bản ghi đó
                        Entity updateRequest = new Entity(tableName, entity.Id);
                        updateRequest[targetField] = new OptionSetValue(b.Value);

                        service.Update(updateRequest);
                        count++;

                        if (count % 10 == 0)
                            Console.WriteLine($"Đã xử lý: {count}/{records.Entities.Count}");
                    }
                }

                Console.WriteLine($"\n=== THÀNH CÔNG: Đã cập nhật {count} bản ghi ===");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Lỗi thực thi: " + ex.Message);
            }
            Console.ReadLine();
        }
    }
}