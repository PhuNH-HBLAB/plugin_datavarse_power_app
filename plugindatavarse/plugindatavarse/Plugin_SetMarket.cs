using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Diagnostics;

namespace plugindatavarse
{
    public class Plugin_SetMarket : IPlugin
    {
        private readonly string _targetMarketField;
        private readonly string _targetSalesField;      // Dùng cho Account/Opp

        private readonly string _targetSalesOppField;   // Dùng cho Contract -> Opp
        private readonly string _targetOwnerOppField;   // Dùng cho Contract -> Opp

        private const string UserMarketField = "mc_user_market";

        public Plugin_SetMarket(string unsecureConfig)
        {
            if (!string.IsNullOrWhiteSpace(unsecureConfig))
            {
                var parts = unsecureConfig.Split(',');

                if (parts.Length >= 1) _targetMarketField = parts[0].Trim();

                if (parts.Length == 2)
                {
                    // Case 2 tham số: Cho Account và Opportunity
                    _targetSalesField = parts[1].Trim();
                }
                else if (parts.Length >= 3)
                {
                    // Case 3 tham số: Cho Contract (Market, Sales của Opp, Owner của Opp)
                    _targetSalesOppField = parts[1].Trim();
                    _targetOwnerOppField = parts[2].Trim();
                }
            }
        }

        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

            if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity target)
            {
                int? finalMarketValue = null;

                // --- 1. LOGIC RIÊNG CHO BẢNG CONTRACT ---
                if (target.LogicalName == "hbl_contract")
                {
                    EntityReference oppRef = target.GetAttributeValue<EntityReference>("opportunityid");
                    if (oppRef != null && !string.IsNullOrEmpty(_targetSalesOppField) && !string.IsNullOrEmpty(_targetOwnerOppField))
                    {
                        // Lấy Owner và Sales của Opp theo tham số truyền vào (part[1] và part[2])
                        Entity opp = service.Retrieve("hbl_opportunities", oppRef.Id, new ColumnSet(_targetOwnerOppField, _targetSalesOppField));

                        // Ưu tiên 1: Owner của Opp
                        finalMarketValue = GetMarketValueFromUser(service, opp.GetAttributeValue<EntityReference>(_targetOwnerOppField)?.Id);
                        
                        // Ưu tiên 2: Sales của Opp
                        if (finalMarketValue == null)
                        {
                            finalMarketValue = GetMarketValueFromUser(service, opp.GetAttributeValue<EntityReference>(_targetSalesOppField)?.Id);
                        }
                    }
                }

                // --- 2. LOGIC CHUNG (CHO ACCOUNT, OPP HOẶC CONTRACT KHÔNG CÓ OPP) ---
                if (finalMarketValue == null)
                {
                    // Ưu tiên 3: Owner của chính bản ghi hiện tại
                    finalMarketValue = GetMarketValueFromUser(service, target.GetAttributeValue<EntityReference>("ownerid")?.Id);

                    // Ưu tiên 4: Cột Sales của bản ghi hiện tại (chỉ chạy nếu là Account/Opp vì Contract dùng logic riêng ở trên)
                    if (finalMarketValue == null && !string.IsNullOrEmpty(_targetSalesField))
                    {
                        finalMarketValue = GetMarketValueFromUser(service, target.GetAttributeValue<EntityReference>(_targetSalesField)?.Id);
                    }
                }

                // --- 3. GÁN GIÁ TRỊ ---
                if (finalMarketValue != null && !string.IsNullOrEmpty(_targetMarketField))
                {
                    target[_targetMarketField] = new OptionSetValue(finalMarketValue.Value);
                }
            }
        }

        private int? GetMarketValueFromUser(IOrganizationService service, Guid? userId)
        {
            if (userId == null) return null;
            try
            {
                Entity user = service.Retrieve("systemuser", userId.Value, new ColumnSet(UserMarketField));
                return user.GetAttributeValue<OptionSetValue>(UserMarketField)?.Value;
            }
            catch { return null; }
        }
    }
}