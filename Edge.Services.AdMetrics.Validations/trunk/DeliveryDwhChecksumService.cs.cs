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

    public class DeliveryDwhChecksumService : DeliveryDBChecksumBaseService
    {
        protected override ValidationResult DeliveryDbCompare(Delivery delivery, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable)
        {
            double costDif = 0;
            double impsDif = 0;
            double clicksDif = 0;
            string dayCode = delivery.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd"); // Delivery Per Day = > TargetPeriod.Start = daycode

            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, DbConnectionStringName)))
            {
                // This option is relevant only for account with one commited delivery.
                // for example it is not relevant for Easy forex.
                sqlCon.Open();
                SqlCommand sqlCommand = DataManager.CreateCommand(
               "SELECT SUM(cost),sum(imps),sum(clicks) from " + comparisonTable +
               " where Account_ID = @Account_ID and Day_ID = @Daycode and Channel_ID = @Channel_ID" 
               );

                SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
                SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);
                SqlParameter channelIdParam = new SqlParameter("@Channel_ID", System.Data.SqlDbType.Int);

                accountIdParam.Value = delivery.Account.ID;
                daycodeParam.Value = dayCode;
                channelIdParam.Value = delivery.Channel.ID;

                sqlCommand.Parameters.Add(accountIdParam);
                sqlCommand.Parameters.Add(daycodeParam);
                sqlCommand.Parameters.Add(channelIdParam);

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


                            #region Scenario : data exists in delivery and not in DB

                            else if (totals["Cost"] > 0 || totals["Clicks"] > 0 || totals["Impressions"] > 0)
                                return new ValidationResult()
                                {
                                    ResultType = ValidationResultType.Error,
                                    AccountID = delivery.Account.ID,
                                    TargetPeriodStart = delivery.TargetPeriodStart,
                                    TargetPeriodEnd = delivery.TargetPeriodEnd,
                                    Message = "Data exists in delivery but not in DB for Account Original ID: " + delivery.Account.OriginalID,
                                    ChannelID = delivery.Channel.ID,
                                    CheckType = this.Instance.Configuration.Name
                                };
                            #endregion

                            #region Scenario: Found differences

                            if ((costDif != 0 && (costDif / totals["Cost"] > ALLOWED_DIFF)) ||
                            (clicksDif != 0 && (clicksDif / totals["Clicks"] > ALLOWED_DIFF)) ||
                            (impsDif != 0 && (impsDif / totals["Impressions"] > ALLOWED_DIFF)))

                                return new ValidationResult()
                                {
                                    ResultType = ValidationResultType.Error,
                                    AccountID = delivery.Account.ID,
                                    DeliveryID = delivery.DeliveryID,
                                    TargetPeriodStart = delivery.TargetPeriodStart,
                                    TargetPeriodEnd = delivery.TargetPeriodEnd,
                                    Message = "validation Error - differences has been found - Account Original ID: " + delivery.Account.OriginalID,
                                    ChannelID = delivery.Channel.ID,
                                    CheckType = this.Instance.Configuration.Name
                                };
                            #endregion

                            #region Scenario: Differences were not found
                            else
                                return new ValidationResult()
                                {
                                    ResultType = ValidationResultType.Information,
                                    AccountID = delivery.Account.ID,
                                    DeliveryID = delivery.DeliveryID,
                                    TargetPeriodStart = delivery.TargetPeriodStart,
                                    TargetPeriodEnd = delivery.TargetPeriodEnd,
                                    Message = "validation Success - Account Original ID: " + delivery.Account.OriginalID,
                                    ChannelID = delivery.Channel.ID,
                                    CheckType = this.Instance.Configuration.Name
                                };
                            #endregion

                        }

                    }

                    // If reader is closed
                    else
                        return new ValidationResult()
                        {
                            ResultType = ValidationResultType.Error,
                            AccountID = delivery.Account.ID,
                            DeliveryID = delivery.DeliveryID,
                            TargetPeriodStart = delivery.TargetPeriodStart,
                            TargetPeriodEnd = delivery.TargetPeriodEnd,
                            Message = "Cannot Read Data from DB connection closed - Account Original ID: " + delivery.Account.OriginalID,
                            ChannelID = delivery.Channel.ID,
                            CheckType = this.Instance.Configuration.Name
                        };
                }
                //Could not find check scenario 
                return new ValidationResult()
                {
                    ResultType = ValidationResultType.Error,
                    AccountID = delivery.Account.ID,
                    DeliveryID = delivery.DeliveryID,
                    TargetPeriodStart = delivery.TargetPeriodStart,
                    TargetPeriodEnd = delivery.TargetPeriodEnd,
                    Message = "Could not find check scenario - Account Original ID: " + delivery.Account.OriginalID,
                    ChannelID = delivery.Channel.ID,
                    CheckType = this.Instance.Configuration.Name
                };

            }
        }
    }
}
