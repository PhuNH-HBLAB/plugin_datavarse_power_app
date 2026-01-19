using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace plugindatavarse
{
    public class Plugin_Advanced_Month_Allocation : IPlugin
    {
        // --- CẤU HÌNH TÊN BẢNG & TRƯỜNG ---
        private const string TableA = "hbl_contract";
        private const string TableA_Id = "hbl_contractid";
        private const string Col_A_Year = "mc_contract_year"; // Cột này trong DB là Decimal

        private const string Col_A_InvoiceMonths = "mc_contract_invoiced_months";
        private const string Col_A_PaidMonths = "mc_contract_paid_months";
        private const string Col_A_ContractMonths = "mc_contract_contract_month";

        private const string TableB = "mc_contract_month";
        private const string Col_B_RefA = "mc_contract_ref";
        private const string Col_B_RefC = "mc_month_ref";
        private const string Col_B_Type = "mc_type_month_ref";
        private const string Col_B_Name = "mc_contract_month_name";

        private const string TableC = "hbl_month";
        private const string TableC_Id = "hbl_monthid";
        private const string Col_C_Month = "hbl_month_name";
        private const string Col_C_Year = "mc_month_year"; // Cột này trong DB là Decimal

        private const string Col_C_Sum_Invoice = "mc_month_sum_invoice";
        private const string Col_C_Count_Invoice = "mc_month_count_invoice";
        private const string Col_C_Sum_Paid = "mc_month_sum_paid";
        private const string Col_C_Count_Paid = "mc_month_count_paid";
        private const string Col_C_Sum_Contract = "mc_month_sum_contract";
        private const string Col_C_Count_Contract = "mc_month_count_contract";

        private const int Type_Invoice = 758130000;
        private const int Type_Paid = 758130001;
        private const int Type_Contract = 758130002;

        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            trace.Trace($"[START] Plugin Execute. Message: {context.MessageName}, Depth: {context.Depth}");

            if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity)
            {
                string message = context.MessageName.ToLower();
                if (message != "create" && message != "update") return;

                try
                {
                    Entity target = (Entity)context.InputParameters["Target"];

                    Entity preImage = (context.PreEntityImages.Contains("PreImage")) ? context.PreEntityImages["PreImage"] : new Entity(TableA);
                    Entity postImage = (context.PostEntityImages.Contains("PostImage")) ? context.PostEntityImages["PostImage"] : target;

                    // Lấy năm từ PostImage (đã fix đọc Decimal)
                    int year = GetYear(postImage, trace);
                    trace.Trace($"Processing Year: {year}");

                    for (int month = 1; month <= 12; month++)
                    {
                        ProcessMonth(service, trace, preImage, postImage, month, year, target.Id);
                    }
                    trace.Trace("[END] Plugin Execute Successfully.");
                }
                catch (Exception ex)
                {
                    trace.Trace($"[ERROR] Fatal Plugin Error: {ex.ToString()}");
                    throw new InvalidPluginExecutionException("Error: " + ex.Message);
                }
            }
        }

        private void ProcessMonth(IOrganizationService service, ITracingService trace, Entity preImage, Entity postImage, int month, int year, Guid recordAId)
        {
            bool isInvoice_Old = IsMonthSelected(preImage, Col_A_InvoiceMonths, month, trace, "Pre-Invoice");
            bool isInvoice_New = IsMonthSelected(postImage, Col_A_InvoiceMonths, month, trace, "Post-Invoice");

            bool isPaid_Old = IsMonthSelected(preImage, Col_A_PaidMonths, month, trace, "Pre-Paid");
            bool isPaid_New = IsMonthSelected(postImage, Col_A_PaidMonths, month, trace, "Post-Paid");

            bool isContract_Old = IsMonthSelected(preImage, Col_A_ContractMonths, month, trace, "Pre-Contract");
            bool isContract_New = IsMonthSelected(postImage, Col_A_ContractMonths, month, trace, "Post-Contract");

            if (!isInvoice_New && !isPaid_New && !isContract_New &&
                !isInvoice_Old && !isPaid_Old && !isContract_Old)
            {
                return;
            }

            trace.Trace($"[ACTION] Month {month} has changes. Processing...");

            List<int> activeTypes = new List<int>();
            if (isInvoice_New) activeTypes.Add(Type_Invoice);
            if (isPaid_New) activeTypes.Add(Type_Paid);
            if (isContract_New) activeTypes.Add(Type_Contract);

            Guid tableCId = EnsureTableCExists(service, trace, month, year);
            SyncTableB(service, trace, new EntityReference(TableA, recordAId), new EntityReference(TableC, tableCId), activeTypes, month);

            string amountCol = GetAmountColumnByMonth(month);
            decimal amount_Old = GetDecimalValue(preImage, amountCol);
            decimal amount_New = GetDecimalValue(postImage, amountCol);

            trace.Trace($"Amount ({amountCol}): Old={amount_Old}, New={amount_New}");

            decimal dMoney_Inv = CalculateDeltaMoney(isInvoice_Old, isInvoice_New, amount_Old, amount_New);
            decimal dMoney_Paid = CalculateDeltaMoney(isPaid_Old, isPaid_New, amount_Old, amount_New);
            decimal dMoney_Cont = CalculateDeltaMoney(isContract_Old, isContract_New, amount_Old, amount_New);

            int dCount_Inv = CalculateDeltaCount(isInvoice_Old, isInvoice_New);
            int dCount_Paid = CalculateDeltaCount(isPaid_Old, isPaid_New);
            int dCount_Cont = CalculateDeltaCount(isContract_Old, isContract_New);

            if (dMoney_Inv != 0 || dCount_Inv != 0 || dMoney_Paid != 0 || dCount_Paid != 0 || dMoney_Cont != 0 || dCount_Cont != 0)
            {
                UpdateTableC_Delta(service, tableCId,
                    dMoney_Inv, dCount_Inv,
                    dMoney_Paid, dCount_Paid,
                    dMoney_Cont, dCount_Cont);
            }
        }

        private decimal GetDecimalValue(Entity entity, string attributeName)
        {
            if (!entity.Contains(attributeName) || entity[attributeName] == null) return 0m;
            object val = entity[attributeName];

            if (val is Money m) return m.Value;
            if (val is decimal d) return d;
            if (val is double db) return (decimal)db;
            if (val is int i) return (decimal)i;

            return 0m;
        }

        private decimal CalculateDeltaMoney(bool wasSelected, bool isSelected, decimal oldVal, decimal newVal)
        {
            if (!wasSelected && isSelected) return newVal;
            if (wasSelected && !isSelected) return -oldVal;
            if (wasSelected && isSelected) return newVal - oldVal;
            return 0m;
        }

        private int CalculateDeltaCount(bool wasSelected, bool isSelected)
        {
            if (!wasSelected && isSelected) return 1;
            if (wasSelected && !isSelected) return -1;
            return 0;
        }

        private void SyncTableB(IOrganizationService service, ITracingService trace, EntityReference refA, EntityReference refC, List<int> activeTypes, int month)
        {
            QueryExpression query = new QueryExpression(TableB);
            query.ColumnSet = new ColumnSet(TableB + "id");
            query.Criteria.AddCondition(Col_B_RefA, ConditionOperator.Equal, refA.Id);
            query.Criteria.AddCondition(Col_B_RefC, ConditionOperator.Equal, refC.Id);
            EntityCollection results = service.RetrieveMultiple(query);

            if (activeTypes.Count == 0)
            {
                if (results.Entities.Count > 0) service.Delete(TableB, results.Entities[0].Id);
                return;
            }

            object typeCollection = null;
            try
            {
                Type oscType = Type.GetType("Microsoft.Xrm.Sdk.OptionSetValueCollection, Microsoft.Xrm.Sdk")
                            ?? Type.GetType("Microsoft.Xrm.Sdk.OptionSetCollection, Microsoft.Xrm.Sdk");

                if (oscType != null)
                {
                    typeCollection = Activator.CreateInstance(oscType);
                    System.Reflection.MethodInfo addMethod = oscType.GetMethod("Add");
                    foreach (int t in activeTypes)
                    {
                        addMethod.Invoke(typeCollection, new object[] { new OptionSetValue(t) });
                    }
                }
            }
            catch (Exception ex) { trace.Trace("Reflection Error: " + ex.Message); }

            string monthName = GetMonthName(month);

            if (results.Entities.Count > 0)
            {
                Entity updateB = new Entity(TableB, results.Entities[0].Id);
                if (typeCollection != null) updateB[Col_B_Type] = typeCollection;
                updateB[Col_B_Name] = monthName;
                service.Update(updateB);
            }
            else
            {
                Entity newB = new Entity(TableB);
                newB[Col_B_RefA] = refA;
                newB[Col_B_RefC] = refC;
                newB[Col_B_Name] = monthName;
                if (typeCollection != null) newB[Col_B_Type] = typeCollection;
                service.Create(newB);
            }
        }

        private void UpdateTableC_Delta(IOrganizationService service, Guid tableCId,
            decimal dMoneyInv, int dCountInv,
            decimal dMoneyPaid, int dCountPaid,
            decimal dMoneyCont, int dCountCont)
        {
            Entity currentC = service.Retrieve(TableC, tableCId, new ColumnSet(
                Col_C_Sum_Invoice, Col_C_Count_Invoice,
                Col_C_Sum_Paid, Col_C_Count_Paid,
                Col_C_Sum_Contract, Col_C_Count_Contract));

            Entity updateC = new Entity(TableC, tableCId);

            ApplyDelta(currentC, updateC, Col_C_Sum_Invoice, Col_C_Count_Invoice, dMoneyInv, dCountInv);
            ApplyDelta(currentC, updateC, Col_C_Sum_Paid, Col_C_Count_Paid, dMoneyPaid, dCountPaid);
            ApplyDelta(currentC, updateC, Col_C_Sum_Contract, Col_C_Count_Contract, dMoneyCont, dCountCont);

            service.Update(updateC);
        }

        private void ApplyDelta(Entity current, Entity update, string colSum, string colCount, decimal dMoney, int dCount)
        {
            if (dMoney != 0)
            {
                decimal currVal = current.Contains(colSum) ? (decimal)current[colSum] : 0m;
                update[colSum] = currVal + dMoney;
            }
            if (dCount != 0)
            {
                // Fix: Đọc decimal, cộng int, lưu decimal
                decimal currVal = 0m;
                if (current.Contains(colCount) && current[colCount] != null)
                {
                    if (current[colCount] is decimal d) currVal = d;
                    else if (current[colCount] is int i) currVal = (decimal)i;
                }
                update[colCount] = currVal + (decimal)dCount;
            }
        }

        private bool IsMonthSelected(Entity entity, string colName, int monthToCheck, ITracingService trace, string tag)
        {
            List<int> selectedMonths = GetMultiSelectValues(entity, colName, trace, tag);
            return selectedMonths.Contains(monthToCheck);
        }

        private List<int> GetMultiSelectValues(Entity entity, string attributeName, ITracingService trace, string tag)
        {
            List<int> results = new List<int>();
            if (!entity.Contains(attributeName) || entity[attributeName] == null) return results;

            object val = entity[attributeName];
            string typeName = val.GetType().Name;

            if (typeName == "OptionSetCollection" || typeName == "OptionSetValueCollection")
            {
                IEnumerable list = val as IEnumerable;
                if (list != null)
                {
                    foreach (var item in list)
                    {
                        if (item is OptionSetValue itemOsv)
                        {
                            int m = GetMonthFromOptionValue(itemOsv.Value);
                            if (m != -1) results.Add(m);
                        }
                    }
                }
            }
            else if (val is OptionSetValue osv)
            {
                int m = GetMonthFromOptionValue(osv.Value);
                if (m != -1) results.Add(m);
            }
            return results;
        }

        private int GetMonthFromOptionValue(int dataverseValue)
        {
            if (dataverseValue >= 1 && dataverseValue <= 12) return dataverseValue;
            int baseValue = 758130000;
            if (dataverseValue >= baseValue && dataverseValue <= (baseValue + 100))
                return (dataverseValue - baseValue) + 1;
            return -1;
        }

        // --- HÀM GET YEAR ĐÃ SỬA ---
        private int GetYear(Entity entity, ITracingService trace)
        {
            if (entity.Contains(Col_A_Year) && entity[Col_A_Year] != null)
            {
                object val = entity[Col_A_Year];
                // Ưu tiên đọc Decimal (vì DB là Decimal)
                if (val is decimal d) return (int)d;
                if (val is double db) return (int)db;
                if (val is int i) return i;
                if (val is OptionSetValue opt) return opt.Value;

                // Fallback: Parse string nếu cần
                if (decimal.TryParse(val.ToString(), out decimal parsed)) return (int)parsed;
            }
            trace.Trace("Year not found in Entity, defaulting to DateTime.Now.Year");
            return DateTime.Now.Year;
        }

        private Guid EnsureTableCExists(IOrganizationService service, ITracingService trace, int month, int year)
        {
            string monthName = GetMonthName(month);

            // Year: Convert to Decimal để query
            object yearValue = Convert.ToDecimal(year);

            QueryExpression query = new QueryExpression(TableC);
            query.ColumnSet = new ColumnSet(TableC_Id);
            query.Criteria.AddCondition(Col_C_Month, ConditionOperator.Equal, monthName);
            query.Criteria.AddCondition(Col_C_Year, ConditionOperator.Equal, yearValue);

            EntityCollection results = service.RetrieveMultiple(query);
            if (results.Entities.Count > 0) return results.Entities[0].Id;

            trace.Trace($"Creating NEW Table C for {monthName}/{year}");
            Entity newC = new Entity(TableC);
            newC[Col_C_Month] = monthName;
            newC[Col_C_Year] = yearValue;

            newC[Col_C_Sum_Invoice] = 0m;
            newC[Col_C_Count_Invoice] = 0m;
            newC[Col_C_Sum_Paid] = 0m;
            newC[Col_C_Count_Paid] = 0m;
            newC[Col_C_Sum_Contract] = 0m;
            newC[Col_C_Count_Contract] = 0m;

            return service.Create(newC);
        }

        private string GetMonthName(int month)
        {
            switch (month)
            {
                case 1: return "Jan";
                case 2: return "Feb";
                case 3: return "Mar";
                case 4: return "Apr";
                case 5: return "May";
                case 6: return "Jun";
                case 7: return "Jul";
                case 8: return "Aug";
                case 9: return "Sep";
                case 10: return "Oct";
                case 11: return "Nov";
                case 12: return "Dec";
                default: return "";
            }
        }

        private string GetAmountColumnByMonth(int month)
        {
            switch (month)
            {
                case 1: return "hbl_contract_jan";
                case 2: return "hbl_contract_feb";
                case 3: return "hbl_contract_mar";
                case 4: return "hbl_contract_apr";
                case 5: return "hbl_contract_may";
                case 6: return "hbl_contract_jun";
                case 7: return "hbl_contract_jul";
                case 8: return "hbl_contract_aug";
                case 9: return "hbl_contract_sep";
                case 10: return "hbl_contract_oct";
                case 11: return "hbl_contract_nov";
                case 12: return "hbl_contract_dec";
                default: return "hbl_contract_jan";
            }
        }
    }
}