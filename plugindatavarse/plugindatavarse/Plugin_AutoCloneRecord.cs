using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace plugindatavarse
{
    public class Plugin_AutoCloneRecord : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            // 1. Khởi tạo ngữ cảnh (Context)
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
            ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            // 2. Kiểm tra xem có phải là Message Create và bảng hbl_account_planning không
            if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity)
            {
                Entity sourceEnt = (Entity)context.InputParameters["Target"];

                // Đảm bảo đây là bảng A (hbl_account_planning)
                if (sourceEnt.LogicalName != "hbl_account_planning") return;

                try
                {
                    tracingService.Trace("Bắt đầu clone record sang bảng mc_account_actual...");

                    // 3. Khởi tạo đối tượng bảng B
                    Entity targetEnt = new Entity("mc_account_actual");

                    // 4. Ánh xạ các trường (Mapping) dựa theo ảnh bạn gửi

                    // Các trường thông tin chung
                    CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_market", "mc_account_actual_market");
                    CopyAttribute(sourceEnt, targetEnt, "hblab_account_planning_account", "mc_actual_account");
                    CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_resouce_type", "mc_account_actual_resouce_type");
                    CopyAttribute(sourceEnt, targetEnt, "hbl_account_plan_unit_price", "mc_account_actual_unit_price");

                    // Các trường tháng mm1 -> mm12
                    for (int i = 1; i <= 12; i++)
                    {
                        CopyAttribute(sourceEnt, targetEnt, $"hbl_mm{i}", $"mc_mm{i}");
                    }

                    // Các trường khác
                    CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_opportunity", "mc_account_actual_opportunity");
                    CopyAttribute(sourceEnt, targetEnt, "hbl_account_planning_certainty", "mc_account_actual_certainty");
                    CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_service_offering", "mc_account_actual_service_offering");
                    CopyAttribute(sourceEnt, targetEnt, "hbl_account_planning_currency", "mc_account_actual_currency");
                    CopyAttribute(sourceEnt, targetEnt, "hbl_link_acc_plan", "mc_account_actual_link_accPlan");
                    CopyAttribute(sourceEnt, targetEnt, "hbl_account_planning_year", "mc_Year");
                    CopyAttribute(sourceEnt, targetEnt, "hbl_Newcolumn", "mc_account_actual_name");
                    CopyAttribute(sourceEnt, targetEnt, "mc_account_planning_ap_type", "mc_account_actual_ap_type");

                    // 5. Thực hiện tạo bản ghi ở bảng B
                    service.Create(targetEnt);

                    tracingService.Trace("Tạo record bảng B thành công.");
                }
                catch (Exception ex)
                {
                    tracingService.Trace("Lỗi Plugin: " + ex.ToString());
                    throw new InvalidPluginExecutionException("Có lỗi xảy ra trong quá trình tự động tạo bản ghi ở bảng B: " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Hàm hỗ trợ copy dữ liệu nếu trường ở bảng A có giá trị
        /// </summary>
        private void CopyAttribute(Entity source, Entity target, string sourceAttr, string targetAttr)
        {
            if (source.Contains(sourceAttr))
            {
                target[targetAttr] = source[sourceAttr];
            }
        }
    }
}