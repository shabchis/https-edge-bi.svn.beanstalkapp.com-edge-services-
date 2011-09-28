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
        private static Double ALLOWED_DIFF = 0.1;
        private double progress = 0;
        protected override IEnumerable<ValidationResult> Validate()
        {
            Channel channel = new Channel();
            progress += 0.1;
            this.ReportProgress(progress);

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

            foreach (DeliverySearchItem deliveryToSearch in deliverySearchList)
            {
                //Getting matched deliveries
                Delivery[] deliveriesToCheck = Delivery.GetByTargetPeriod(deliveryToSearch.targetPeriod.Start.ToDateTime(), deliveryToSearch.targetPeriod.End.ToDateTime(), deliveryToSearch.channel, deliveryToSearch.account);

                progress += 0.3 / 1 - progress;
                this.ReportProgress(progress);

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
                            string dayCode = deliveryToSearch.targetPeriod.Start.ToDateTime().ToString("yyyyMMdd");

                            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "CompareDB")))
                            {
                                sqlCon.Open();
                                SqlCommand sqlCommand = DataManager.CreateCommand(
                                    "SELECT SUM(cost),sum(imps),sum(clicks) from " + comparisonTable +
                                    " where account_id =" + d.Account.ID +
                                    " and Day_Code =" + dayCode +
                                    " and Channel_ID = " + d.Channel.ID + 
                                    " and [Account_ID_SRC] ='"+d.Account.OriginalID+"'"
                                    );

                                sqlCommand.Connection = sqlCon;

                                using (var _reader = sqlCommand.ExecuteReader())
                                {
                                    progress += 0.5 / 1 - progress;
                                    this.ReportProgress(progress);

                                    if (!_reader.IsClosed)
                                    {
                                        while (_reader.Read())
                                        {
                                            if (!_reader[0].Equals(DBNull.Value))
                                            {
                                                costDif = Math.Abs(Convert.ToUInt64(_reader[0]) - totals["Cost"]);
                                                clicksDif = Math.Abs(Convert.ToUInt64(_reader[2]) - totals["Clicks"]);
                                                impsDif = Math.Abs(Convert.ToUInt64(_reader[1]) - totals["Impressions"]);
                                            }

                                            // if data exists in delivery and not in DB
                                            else if (totals["Cost"] > 0 || totals["Clicks"] > 0 || totals["Impressions"] > 0)
                                                yield return new ValidationResult()
                                                {
                                                    ResultType = ValidationResultType.Error,
                                                    AccountID = d.Account.ID,
                                                    TargetPeriodStart = d.TargetPeriodStart,
                                                    TargetPeriodEnd = d.TargetPeriodEnd,
                                                    Message = "Data exists in delivery but not in DB for Account Original ID: "+ d.Account.OriginalID,
                                                    ChannelID = d.Channel.ID,
                                                    CheckType = ValidationCheckType.DeliveryOltp
                                                };

                                            // data exists in both delivery and DB - checking Diff
                                            if ((costDif != 0 && (costDif / totals["Cost"] > ALLOWED_DIFF)) ||
                                                (clicksDif != 0 && (clicksDif / totals["Clicks"] > ALLOWED_DIFF)) ||
                                                (impsDif != 0 && (impsDif / totals["Impressions"] > ALLOWED_DIFF)))

                                                yield return new ValidationResult()
                                                {
                                                    ResultType = ValidationResultType.Error,
                                                    AccountID = deliveryToSearch.account.ID,
                                                    DeliveryID = d.DeliveryID,
                                                    TargetPeriodStart = d.TargetPeriodStart,
                                                    TargetPeriodEnd = d.TargetPeriodEnd,
                                                    Message = "validation Error - differences has been found - Account Original ID: "+ d.Account.OriginalID,
                                                    ChannelID = d.Channel.ID,
                                                    CheckType = ValidationCheckType.DeliveryOltp
                                                };

                                            // No errors then success
                                            else
                                                yield return new ValidationResult()
                                                {
                                                    ResultType = ValidationResultType.Information,
                                                    AccountID = deliveryToSearch.account.ID,
                                                    DeliveryID = d.DeliveryID,
                                                    TargetPeriodStart = d.TargetPeriodStart,
                                                    TargetPeriodEnd = d.TargetPeriodEnd,
                                                    Message = "validation Success - Account Original ID: " + d.Account.OriginalID,
                                                    ChannelID = d.Channel.ID,
                                                    CheckType = ValidationCheckType.DeliveryOltp
                                                };
                                        }

                                    }
                                }
                            }
                        }

                    }
                    else  //if deliveries were not found
                    {
                        yield return new ValidationResult()
                        {
                            ResultType = ValidationResultType.Warning,
                            AccountID = d.Account.ID,
                            TargetPeriodStart = d.TargetPeriodStart,
                            TargetPeriodEnd = d.TargetPeriodEnd,
                            Message = String.Format("Cannot find commited deliveries for Account Original ID: {0} in DB",d.Account.OriginalID),
                            ChannelID = d.Channel.ID,
                            CheckType = ValidationCheckType.DeliveryOltp
                        };
                    }
                }

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

    public class DeliverySearchItem
    {
        public Account account { set; get; }
        public Channel channel { set; get; }
        public DateTimeRange targetPeriod { set; get; }
    }

}
