using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Metrics.Checksums;
using Edge.Data.Pipeline;

namespace Edge.Services.AdMetrics.Validations
{
    class DwhOltpChecksumService : DbDbChecksumBaseService
    {
		protected override ValidationResult DeliveryDbCompare(DeliveryOutput deliveryOutput, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable)
		{

            Dictionary<string, double> oltpTotals = new Dictionary<string, double>();
            Dictionary<string, double> dwhTotals = new Dictionary<string, double>();

            string dayCode = Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd");

            #region Get Totals from Oltp


            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "OltpDB")))
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
            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "DwhDB")))
            {
                sqlCon.Open();

                SqlCommand sqlCommand = new SqlCommand(
                   "SELECT SUM(cost),sum(Impressions),sum(clicks) from " + DwhTabel +
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

            return IsEqual(Params, oltpTotals, dwhTotals,"Oltp","Dwh");
            #endregion
        }

     
    }
}