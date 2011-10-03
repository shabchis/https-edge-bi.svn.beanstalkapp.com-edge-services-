using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.AdMetrics.Validations
{
    class DwhOltpChecksumService : DbDbChecksumBaseService
    {
        protected override Data.Pipeline.Services.ValidationResult Compare(string OltpDB, string OltpTable, string DwhDB, string DwhTabel, Dictionary<string, string> Params)
        {

            Dictionary<string, double> oltpTotals = new Dictionary<string, double>();
            Dictionary<string, double> dwhTotals = new Dictionary<string, double>();
           

            string dayCode = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");

            #region Get Totals from Oltp


            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, OltpDB)))
            {
                sqlCon.Open();

                SqlCommand sqlCommand = new SqlCommand(
                   "SELECT SUM(cost),sum(imps),sum(clicks) from " + OltpTable +
                   " where account_id = @Account_ID and Day_Code = @Daycode and Channel_ID = @Channel_ID"
                   );

                SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
                SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);
                SqlParameter channelIdParam = new SqlParameter("@Channel_ID", System.Data.SqlDbType.Int);

                accountIdParam.Value = Params["AccountID"];
                daycodeParam.Value = dayCode;
                channelIdParam.Value = Params["ChannelID"];

                sqlCommand.Parameters.Add(accountIdParam);
                sqlCommand.Parameters.Add(daycodeParam);
                sqlCommand.Parameters.Add(channelIdParam);

                sqlCommand.Connection = sqlCon;

                using (var _reader = sqlCommand.ExecuteReader())
                {
                    progress += 0.5 * (1 - progress);
                    this.ReportProgress(progress);

                    if (!_reader.IsClosed)
                    {
                        while (_reader.Read())
                        {
                            if (!_reader[0].Equals(DBNull.Value))
                            {
                                oltpTotals.Add("Cost", Convert.ToDouble(_reader[0]));
                                oltpTotals.Add("Imps", Convert.ToDouble(_reader[1]));
                                oltpTotals.Add("Clicks", Convert.ToDouble(_reader[2]));
                            }
                        }

                    }

                }
            }
            #endregion

            #region Get Totals from DWH
            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, OltpDB)))
            {
                sqlCon.Open();

                SqlCommand sqlCommand = new SqlCommand(
                   "SELECT SUM(cost),sum(imps),sum(clicks) from " + OltpTable +
                   " where account_id = @Account_ID and Day_ID = @Daycode and Channel_ID = @Channel_ID"
                   );

                SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
                SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);
                SqlParameter channelIdParam = new SqlParameter("@Channel_ID", System.Data.SqlDbType.Int);

                accountIdParam.Value = Params["AccountID"];
                daycodeParam.Value = dayCode;
                channelIdParam.Value = Params["ChannelID"];

                sqlCommand.Parameters.Add(accountIdParam);
                sqlCommand.Parameters.Add(daycodeParam);
                sqlCommand.Parameters.Add(channelIdParam);

                sqlCommand.Connection = sqlCon;

                using (var _reader = sqlCommand.ExecuteReader())
                {
                    progress += 0.5 * (1 - progress);
                    this.ReportProgress(progress);

                    if (!_reader.IsClosed)
                    {
                        while (_reader.Read())
                        {
                            if (!_reader[0].Equals(DBNull.Value))
                            {
                                dwhTotals.Add("Cost", Convert.ToDouble(_reader[0]));
                                dwhTotals.Add("Imps", Convert.ToDouble(_reader[1]));
                                dwhTotals.Add("Clicks", Convert.ToDouble(_reader[2]));
                            }
                        }

                    }
                }
            }
            #endregion
            #region Comparing totals results

            bool costAlert = false;
            bool impsAlert = false;
            bool clicksAlert = false;

            double costDif = 0;
            double impsDif = 0;
            double clicksDif = 0;

            if ((costDif = Math.Abs(dwhTotals["Cost"] - oltpTotals["Cost"])) > 1) costAlert = true;
            if ((impsDif = Math.Abs(dwhTotals["Imps"] - oltpTotals["Cost"])) > 1) impsAlert = true;
            if ((clicksDif = Math.Abs(dwhTotals["Clicks"] - oltpTotals["Cost"])) > 1) clicksAlert = true;


            StringBuilder message = new StringBuilder();
            message.Append(string.Format("Error - Differences has been found for Account ID {0} : ", Params["AccountID"]));
            if (costAlert) message.Append(string.Format(" OltpCost: {0},DwhCost: {1}, Diff:{3} ", oltpTotals["Cost"],dwhTotals["Cost"], costDif));
            if (impsAlert) message.Append(string.Format(" OltpImps: {0},DwhImps: {1}, Diff:{3} ", oltpTotals["Imps"], dwhTotals["Imps"], impsDif));
            if (clicksAlert) message.Append(string.Format(" OltpClicks: {0},DwhClicks: {1}, Diff:{3} ", oltpTotals["Clicks"], dwhTotals["Clicks"], clicksDif));


            
            if (costAlert || impsAlert || clicksAlert)
                return new ValidationResult()
                                    {
                                        ResultType = ValidationResultType.Error,
                                        AccountID = Convert.ToInt32(Params["AccountID"]),
                                        Message = message.ToString(),
                                        ChannelID = Convert.ToInt32(Params["ChannelID"]),
                                        CheckType = ValidationCheckType.OltpDwh
                                    };
            else return new ValidationResult()
            {
                ResultType = ValidationResultType.Information,
                AccountID = Convert.ToInt32(Params["AccountID"]),
                Message = "Validation Success - no differences",
                ChannelID = Convert.ToInt32(Params["ChannelID"]),
                CheckType = ValidationCheckType.OltpDwh
            };
            #endregion
        }
    }
}