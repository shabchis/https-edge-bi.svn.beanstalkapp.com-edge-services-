using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;

namespace Edge.Services.AdMetrics.Validations
{
    public class DeliveryOltpChecksumService : ValidationService
    {
        protected override IEnumerable<ValidationResult> Validate()
        {
            Channel channel = new Channel();

            //Getting Accounts list
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["AccountsList"]))
                throw new Exception("Missing Configuration option AccountsList");
            string[] accounts = this.Instance.Configuration.Options["AccountsList"].Split(',');

            //Getting Table 
            string comparisonTable;
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["ComparisonTable"]))
                throw new Exception("Missing Configuration option ComparisonTable");
            else comparisonTable = this.Instance.Configuration.Options["ComparisonTable"];

            //Getting Channel List
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["ChannelList"]))
                throw new Exception("Missing Configuration option ChannelList");
            string[] channels = this.Instance.Configuration.Options["ChannelList"].Split(',');

            //Getting TimePeriod
            DateTime fromDate = this.TargetPeriod.Start.ToDateTime();
            DateTime toDate = this.TargetPeriod.End.ToDateTime();
            List<DeliverySearchItem> deliverySearchList = new List<DeliverySearchItem>();

            while (fromDate <= toDate)
            {
                // {start: {base : '2009-01-01', h:0}, end: {base: '2009-01-01', h:'*'}}
                var subRange = new DateTimeRange()
                    {
                        Start = new DateTimeSpecification()
                        {
                            BaseDateTime = fromDate,
                            Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Exact, Value = 0 },
                            Boundary = DateTimeSpecificationBounds.Lower
                        },

                        End = new DateTimeSpecification()
                        {
                            BaseDateTime = fromDate,
                            Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Max },
                            Boundary = DateTimeSpecificationBounds.Upper
                        }
                    };
              
                foreach (var Channel in channels)
                {
                   
                    foreach (string account in accounts)
                    {
                        DeliverySearchItem delivery = new DeliverySearchItem();
                        delivery.account = new Account() { ID = Convert.ToInt16(account) };
                        delivery.channel = new Channel() { ID = Convert.ToInt16(Channel) };
                        delivery.targetPeriod = subRange;
                        deliverySearchList.Add(delivery);
                    }
                }
                fromDate = fromDate.AddDays(1);
            }

            foreach (DeliverySearchItem deliveryToSearch in deliverySearchList)
            {
                //Getting matched deliveries
                Delivery[] deliveriesToCheck = Delivery.GetByTargetPeriod(deliveryToSearch.targetPeriod.Start.ToDateTime(), deliveryToSearch.targetPeriod.End.ToDateTime(), deliveryToSearch.channel, deliveryToSearch.account);
              
                foreach (Delivery d in deliveriesToCheck)
                {
                    int rollbackIndex = -1;
                    int commitIndex = -1;
                    for (int i = 0; i < d.History.Count; i++)
                    {
                        if (d.History[i].Operation == DeliveryOperation.Committed)
                            commitIndex = i;
                        else if (d.History[i].Operation == DeliveryOperation.RolledBack)
                            rollbackIndex = i;
                    }

                    if (commitIndex > rollbackIndex)
                    {

                        object totalso;
                        DeliveryHistoryEntry commitEntry = null;
                        IEnumerable<DeliveryHistoryEntry> processedEntries = d.History.Where(entry => (entry.Operation == DeliveryOperation.Imported));
                        if (processedEntries != null && processedEntries.Count() > 0)
                            commitEntry = (DeliveryHistoryEntry)processedEntries.Last();
                        else
                            continue;
                        if (commitEntry.Parameters.TryGetValue(Edge.Services.AdMetrics.AdMetricsImportManager.Consts.DeliveryHistoryParameters.ChecksumTotals, out totalso))
                        {
                            var totals = (Dictionary<string, double>)totalso;

                            double costDif = 0;
                            double impsDif = 0;
                            double clicksDif = 0;


                            //Check data vs OLTP
                            ValidationResult validationResult = new ValidationResult();
                            string dayCode = String.Format("yyyyMMdd", deliveryToSearch.targetPeriod.Start.ToDateTime());

                            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString("Edge.Core.Services", "SystemDatabase")))
                            {
                                sqlCon.Open();
                                SqlCommand sqlCommand = DataManager.CreateCommand(
                                    "SELECT SUM(cost),sum(imps),sum(clicks) from " + comparisonTable +
                                    " where account_id =" + deliveryToSearch.account.ID +
                                    " and Day_Code =" + dayCode +
                                    " and Channel_ID = " + channel.ID);

                                sqlCommand.Connection = sqlCon;

                                using (var _reader = sqlCommand.ExecuteReader())
                                {
                                    if (!_reader.IsClosed)
                                    {
                                        while (_reader.Read())
                                        {
                                            costDif = Convert.ToInt64(_reader[0]) - totals["cost"];
                                            clicksDif = Convert.ToInt64(_reader[1]) - totals["clicks"];
                                            impsDif = Convert.ToInt64(_reader[2]) - totals["impressions"];
                                        }
                                        if (costDif != 0 || clicksDif != 0 || impsDif != 0)
                                        {
                                            validationResult.ResultType = ValidationResultType.Error;
                                            validationResult.AccountID = deliveryToSearch.account.ID;
                                            validationResult.DeliveryID = d.DeliveryID;
                                            validationResult.TargetPeriodStart = d.TargetPeriodStart;
                                            validationResult.TargetPeriodEnd = d.TargetPeriodEnd;
                                            validationResult.Message = "validation Error";
                                        }
                                        else
                                        {
                                            validationResult.ResultType = ValidationResultType.Information;
                                            validationResult.AccountID = deliveryToSearch.account.ID;
                                            validationResult.DeliveryID = d.DeliveryID;
                                            validationResult.TargetPeriodStart = d.TargetPeriodStart;
                                            validationResult.TargetPeriodEnd = d.TargetPeriodEnd;
                                            validationResult.Message = "validation Success";
                                        }
                                        yield return new ValidationResult();
                                    }
                                }
                            }
                        }

                    }
                }
            }





        }
    }
    public class DeliverySearchItem
    {
        public Account account { set; get; }
        public Channel channel { set; get; }
        public DateTimeRange targetPeriod { set; get; }
    }

}
