using System;
using System.Collections.Generic;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace plugindatavarse
{
    public class Plugin_CountChildRecords : IPlugin
    {
        private readonly string _unsecureConfig;
        private readonly string _secureConfig;

        public Plugin_CountChildRecords(string unsecureConfig, string secureConfig)
        {
            _unsecureConfig = unsecureConfig;
            _secureConfig = secureConfig;
        }

        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(null);

            try
            {
                // 1. CẤU HÌNH (Sửa lại tên trường cho đúng với hệ thống của bạn)
                string lookupField = "hbl_contact_account";  // Lookup con trỏ về cha
                string parentTable = "hbl_account";         // Tên bảng cha
                string countField = "hbl_account_total_contacts"; // Trường lưu số lượng

                // Đọc config nếu có (để linh động)
                if (!string.IsNullOrWhiteSpace(_unsecureConfig))
                {
                    string[] parts = _unsecureConfig.Split(',');
                    if (parts.Length >= 1) lookupField = parts[0].Trim();
                    if (parts.Length >= 2) parentTable = parts[1].Trim();
                    if (parts.Length >= 3) countField = parts[2].Trim();
                }

                List<Guid> parentIdsToUpdate = new List<Guid>();
                string message = context.MessageName.ToLower();

                // 2. LOGIC LẤY ID CHA CẦN TÍNH TOÁN
                // Trường hợp CREATE: Lấy cha từ dữ liệu nhập vào (Target)
                if (message == "create" && context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity target)
                {
                    if (target.Contains(lookupField))
                        parentIdsToUpdate.Add(target.GetAttributeValue<EntityReference>(lookupField).Id);
                }
                // Trường hợp DELETE: Lấy cha từ dữ liệu trước khi xóa (PreImage)
                else if (message == "delete")
                {
                    if (context.PreEntityImages.Contains("PreImage") && context.PreEntityImages["PreImage"] is Entity preImage)
                    {
                        if (preImage.Contains(lookupField))
                            parentIdsToUpdate.Add(preImage.GetAttributeValue<EntityReference>(lookupField).Id);
                    }
                }
                // Trường hợp UPDATE (Đổi cha): Cần tính lại cho cả Cha Cũ và Cha Mới
                else if (message == "update" && context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity updateTarget)
                {
                    // Lấy Cha Mới (từ Target)
                    if (updateTarget.Contains(lookupField))
                        parentIdsToUpdate.Add(updateTarget.GetAttributeValue<EntityReference>(lookupField).Id);

                    // Lấy Cha Cũ (từ PreImage)
                    if (context.PreEntityImages.Contains("PreImage") && context.PreEntityImages["PreImage"] is Entity preImage)
                    {
                        if (preImage.Contains(lookupField))
                            parentIdsToUpdate.Add(preImage.GetAttributeValue<EntityReference>(lookupField).Id);
                    }
                }

                // 3. THỰC HIỆN ĐẾM LẠI VÀ UPDATE
                // Dùng vòng lặp vì lệnh Update có thể ảnh hưởng đến 2 ông cha cùng lúc
                foreach (Guid parentId in parentIdsToUpdate)
                {
                    UpdateParentCount(service, parentId, context.PrimaryEntityName, lookupField, parentTable, countField);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException("Lỗi Plugin Count: " + ex.Message);
            }
        }

        private void UpdateParentCount(IOrganizationService service, Guid parentId, string childTable, string lookupField, string parentTable, string countField)
        {
            // Query đếm số lượng con đang trỏ về cha này
            string fetchXml = $@"
            <fetch distinct='false' mapping='logical' aggregate='true'>
                <entity name='{childTable}'>
                    <attribute name='{childTable}id' alias='cnt' aggregate='count'/>
                    <filter>
                        <condition attribute='{lookupField}' operator='eq' value='{parentId}' />
                    </filter>
                </entity>
            </fetch>";

            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
            int count = 0;
            if (result.Entities.Count > 0 && result.Entities[0].Contains("cnt"))
            {
                count = (int)((AliasedValue)result.Entities[0]["cnt"]).Value;
            }

            // Update số lượng mới vào bảng Cha
            Entity parentUpdate = new Entity(parentTable, parentId);
            parentUpdate[countField] = count;
            service.Update(parentUpdate);
        }
    }
}