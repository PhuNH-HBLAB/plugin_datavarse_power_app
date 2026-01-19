using System;
using Microsoft.Xrm.Sdk;

namespace plugindatavarse
{
    public class Plugin_ContractMeetingCategory : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            // 1. Khởi tạo các dịch vụ cần thiết
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
            ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            // 2. Kiểm tra Target
            if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity)
            {
                Entity target = (Entity)context.InputParameters["Target"];

                // Đảm bảo đúng thực thể hbl_contract
                if (target.LogicalName != "hbl_contract") return;

                try
                {

                    DateTime? endDate = GetValue<DateTime>(target, context, "hbl_contract_end_date");
                    OptionSetValue status = GetValue<OptionSetValue>(target, context, "hbl_contract_status");

                    int statusNormal = 135150001;
                    int statusFinished = 135150006;

                    string resultText = string.Empty;
                    DateTime today = DateTime.Today;

                    if (endDate.HasValue)
                    {
                        TimeSpan diff = endDate.Value.Date - today;
                        int daysDiff = diff.Days;

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

                    target["mc_contract_meeting_category"] = resultText;

                    tracingService.Trace("Logic executed. Result: " + resultText);
                }
                catch (Exception ex)
                {
                    tracingService.Trace("Error: " + ex.ToString());
                    throw new InvalidPluginExecutionException("An error occurred in Plugin_ContractMeetingCategory: " + ex.Message);
                }
            }
        }

        private T GetValue<T>(Entity target, IPluginExecutionContext context, string attributeName)
        {
            if (target.Contains(attributeName))
            {
                return target.GetAttributeValue<T>(attributeName);
            }

            if (context.PreEntityImages.Contains("PreImage") && context.PreEntityImages["PreImage"].Contains(attributeName))
            {
                return context.PreEntityImages["PreImage"].GetAttributeValue<T>(attributeName);
            }

            return default(T);
        }
    }
}