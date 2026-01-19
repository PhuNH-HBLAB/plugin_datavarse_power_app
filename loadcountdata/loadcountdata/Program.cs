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

            try
            {
                CrmServiceClient service = new CrmServiceClient(connectionString);
                if (!service.IsReady)
                {
                    Console.WriteLine("Lỗi kết nối: " + service.LastCrmError);
                    return;
                }
                Console.WriteLine("Kết nối thành công! Đang quét dữ liệu hợp đồng...");

                string tableName = "hbl_contract"; // Tên bảng hợp đồng của bạn
                string targetField = "mc_contract_meeting_category";

                // Khai báo tên Logical Name của các cột điều kiện
                string colEndDate = "hbl_contract_end_date";
                string colStatus = "hbl_contract_status";

                // Khai báo Value của Choice (Cần khớp với hệ thống của bạn)
                int statusNormal = 135150001;
                int statusFinished = 135150006;

                // 1. LẤY DỮ LIỆU CẦN THIẾT
                QueryExpression query = new QueryExpression(tableName);
                query.ColumnSet = new ColumnSet(colEndDate, colStatus); 

                EntityCollection contracts = service.RetrieveMultiple(query);
                Console.WriteLine($"=> Tìm thấy {contracts.Entities.Count} bản ghi cần kiểm tra.");

                int processed = 0;
                DateTime today = DateTime.Today;

                foreach (var con in contracts.Entities)
                {
                    string resultText = string.Empty;

                    DateTime? endDate = con.Contains(colEndDate) ? con.GetAttributeValue<DateTime?>(colEndDate) : null;
                    OptionSetValue status = con.Contains(colStatus) ? con.GetAttributeValue<OptionSetValue>(colStatus) : null;

                    if (endDate.HasValue)
                    {
                        int daysDiff = (endDate.Value.Date - today).Days;

                        
                        if (daysDiff >= 0 && daysDiff <= 30)
                        {
                            resultText = "Ending soon";
                        }
                        else if (status != null && status.Value != statusNormal && status.Value != statusFinished)
                        {
                            resultText = "Status check";
                        }
                        else if (status != null && status.Value == statusNormal && daysDiff < 0)
                        {
                            resultText = "Need Update Contract";
                        }
                    }

                    
                    Entity updateEntity = new Entity(tableName, con.Id);
                    updateEntity[targetField] = resultText;

                    service.Update(updateEntity);
                    processed++;

                    if (processed % 10 == 0) // Hiển thị tiến độ mỗi 10 bản ghi
                        Console.WriteLine($"--- Đã xử lý {processed}/{contracts.Entities.Count} ---");
                }

                Console.WriteLine($"\n=== HOÀN TẤT: Đã cập nhật xong {processed} hợp đồng ===");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Lỗi thực thi: " + ex.Message);
            }
            Console.ReadLine();
        }
    }
}