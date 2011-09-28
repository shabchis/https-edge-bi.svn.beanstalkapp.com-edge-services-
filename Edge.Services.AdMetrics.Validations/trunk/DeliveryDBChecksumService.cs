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
    abstract class DeliveryDBChecksumService  : ValidationService
    {
        abstract protected ValidationResult DeliveryDbCompare(Delivery delivery, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable)();
        public static Double ALLOWED_DIFF = 0.1;
        public double progress = 0;

        protected override IEnumerable<ValidationResult> Validate()
        {
            Channel channel = new Channel();
            progress += 0.1;
            this.ReportProgress(progress);

            #region Getting Service option params
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
            DateTime fromDate, toDate;
            if ((String.IsNullOrEmpty(this.Instance.Configuration.Options["fromDate"])) && (String.IsNullOrEmpty(this.Instance.Configuration.Options["toDate"])))
            {
                fromDate = this.TargetPeriod.Start.ToDateTime();
                toDate = this.TargetPeriod.End.ToDateTime();
            }
            else
            {
                fromDate = Convert.ToDateTime(this.Instance.Configuration.Options["fromDate"]);
                toDate = Convert.ToDateTime(this.Instance.Configuration.Options["toDate"]);
            }
            #endregion

            #region Creating Delivery Search List
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

                        progress += 0.3 / 1 - progress / (channels.LongLength + accounts.LongLength);
                        this.ReportProgress(progress);
                    }
                }
                fromDate = fromDate.AddDays(1);
            }
            #endregion

            foreach (DeliverySearchItem deliveryToSearch in deliverySearchList)
            {
                //Getting criterion matched deliveries
                Delivery[] deliveriesToCheck = Delivery.GetByTargetPeriod(deliveryToSearch.targetPeriod.Start.ToDateTime(), deliveryToSearch.targetPeriod.End.ToDateTime(), deliveryToSearch.channel, deliveryToSearch.account);

                progress += 0.3 / 1 - progress;
                this.ReportProgress(progress);

                foreach (Delivery d in deliveriesToCheck)
                {
                    int rollbackIndex = -1;
                    int commitIndex = -1;

                    #region Searching and researching commited and rolledback deliveries
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
                            Dictionary<string, double> totals = (Dictionary<string, double>)totalso;

                            //Check Delivery data vs OLTP
                            yield return (DeliveryDbCompare(d, totals, "CompareDB", comparisonTable));
                        }

                    }
                    #endregion

                  
                    else  //if could not find Committed and RolledBack deliveries
                    {
                        yield return new ValidationResult()
                        {
                            ResultType = ValidationResultType.Warning,
                            AccountID = d.Account.ID,
                            TargetPeriodStart = d.TargetPeriodStart,
                            TargetPeriodEnd = d.TargetPeriodEnd,
                            Message = String.Format("Cannot find commited deliveries for Account Original ID: {0} in DB", d.Account.OriginalID),
                            ChannelID = d.Channel.ID,
                            CheckType = ValidationCheckType.DeliveryOltp
                        };
                    }
                }

                //could not find deliveries by user criterions
                if (deliveriesToCheck.Length == 0)
                {
                    yield return new ValidationResult()
                    {
                        ResultType = ValidationResultType.Warning,
                        AccountID = deliveryToSearch.account.ID,
                        TargetPeriodStart = deliveryToSearch.targetPeriod.Start.ToDateTime(),
                        TargetPeriodEnd = deliveryToSearch.targetPeriod.End.ToDateTime(),
                        Message = "Cannot find deliveries in DB",
                        ChannelID = deliveryToSearch.channel.ID,
                        CheckType = ValidationCheckType.DeliveryOltp
                    };
                }
            }
        }

       
       
    }
}
