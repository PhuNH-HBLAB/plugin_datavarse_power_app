using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace plugindatavarse
{
    public class Plugin_SetOriginInfo : IPlugin
    {
        private readonly string _unsecureConfig;
        private readonly string _secureConfig;

        // Constructor nhận config từ Plugin Registration Tool
        public Plugin_SetOriginInfo(string unsecureConfig, string secureConfig)
        {
            _unsecureConfig = unsecureConfig;
            _secureConfig = secureConfig;
        }

        public void Execute(IServiceProvider serviceProvider)
        {
            // 1. Lấy các dịch vụ cần thiết
            ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

            // 2. Kiểm tra Target
            if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity)
            {
                Entity entity = (Entity)context.InputParameters["Target"];

                try
                {
                    // 3. Đọc cấu hình từ Unsecure Config
                    // Định dạng mong đợi: "tên_trường_ngày, tên_trường_người_dùng"
                    // Ví dụ: "mc_date_field, mc_user_text_field"

                    if (string.IsNullOrWhiteSpace(_unsecureConfig))
                    {
                        tracingService.Trace("Không có Unsecure Configuration. Plugin dừng lại.");
                        return; // Hoặc throw error nếu bắt buộc phải có config
                    }

                    string[] configParts = _unsecureConfig.Split(',');

                    string fieldDateOrigin = configParts.Length > 0 ? configParts[0].Trim() : null;
                    string fieldUserOrigin = configParts.Length > 1 ? configParts[1].Trim() : null;

                    tracingService.Trace($"Entity: {context.PrimaryEntityName}, FieldDate: {fieldDateOrigin}, FieldUser: {fieldUserOrigin}");

                    // 4. Xử lý logic Ngày (Date)
                    if (!string.IsNullOrEmpty(fieldDateOrigin))
                    {
                        if (!entity.Contains(fieldDateOrigin) || entity[fieldDateOrigin] == null)
                        {
                            entity[fieldDateOrigin] = DateTime.UtcNow;
                            tracingService.Trace("Đã set ngày hiện tại.");
                        }
                    }

                    // 5. Xử lý logic Người dùng (User Text)
                    if (!string.IsNullOrEmpty(fieldUserOrigin))
                    {
                        if (!entity.Contains(fieldUserOrigin) || entity[fieldUserOrigin] == null)
                        {
                            // Lấy thông tin user hiện tại
                            Entity currentUser = service.Retrieve("systemuser", context.UserId, new ColumnSet("firstname", "lastname", "fullname"));

                            string finalName = GetFormattedName(currentUser, tracingService);

                            entity[fieldUserOrigin] = finalName;
                            tracingService.Trace($"Đã set tên người dùng: {finalName}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    tracingService.Trace("Lỗi: " + ex.ToString());
                    throw new InvalidPluginExecutionException($"Lỗi Plugin Set Origin Info ({context.PrimaryEntityName}): {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Tách logic xử lý tên ra hàm riêng cho code gọn và dễ quản lý
        /// </summary>
        private string GetFormattedName(Entity user, ITracingService tracing)
        {
            string firstName = user.GetAttributeValue<string>("firstname");
            string lastName = user.GetAttributeValue<string>("lastname");
            string fullName = user.GetAttributeValue<string>("fullname");

            bool hasFirstName = !string.IsNullOrWhiteSpace(firstName);
            bool hasLastName = !string.IsNullOrWhiteSpace(lastName);

            // Logic cũ của bạn giữ nguyên
            if (!hasFirstName && !hasLastName)
            {
                return fullName;
            }

            if (hasFirstName && firstName.Contains("#"))
            {
                // Nếu FirstName có chứa # (thường là Application User), ưu tiên lấy LastName nếu có
                return hasLastName ? lastName : firstName; // Fallback về firstName nếu không có lastName
            }
            else
            {
                // Nếu bình thường, ưu tiên lấy FirstName
                return hasFirstName ? firstName : fullName;
            }
        }
    }
}