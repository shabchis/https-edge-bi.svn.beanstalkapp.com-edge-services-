using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.AnalysisServices.AdomdClient;
using System.Data;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Data.Pipeline.Services;
using System.Configuration;
using Edge.Data.Pipeline.Services.Common.Validation;

namespace Edge.Services.AdMetrics.Validations
{
    class MdxOltpChecksumService : DbDbChecksumBaseService
    {

        protected override Data.Pipeline.Services.ValidationResult Compare(string SourceTable, string TargetTabel, Dictionary<string, string> Params)
        {

            Dictionary<string, double> oltpTotals = new Dictionary<string, double>();
            Dictionary<string, double> mdxTotals = new Dictionary<string, double>();

            string dayCode = Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd");

            #region Getting measuers from oltp

            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "OltpDB")))
            {
                sqlCon.Open();

                SqlCommand sqlCommand = new SqlCommand(
                   "SELECT SUM(cost),sum(imps),sum(clicks) from " + SourceTable +
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

            #region Getting measures from Analysis server (MDX)

			string admobConnection = this.Instance.Configuration.Options["AdmobConnection"];
			AdomdConnection conn = new AdomdConnection(admobConnection);
            try
            {
                conn.Open();

                //TO DO : Get Cube Name from DB
                string CubeName = GetCubeName(Convert.ToInt32(Params["AccountID"]));

                string mdxCommandText = string.Format(@"Select
                                {{ [Measures].[Impressions],[Measures].[Clicks],[Measures].[Cost]}}
                                    On Columns , 
                                (
	                            [Accounts Dim].[Accounts].[Account].&[{0}]
                                )On Rows 
                                From
                                [{1}]
                                WHERE
                                ([Channels Dim].[Channels].[Channel].&[{2}]
                                ,[Time Dim].[Time Dim].[Day].&[{3}]
                                ) 
                                ", Params["AccountID"], CubeName, Params["ChannelID"], Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd"));

                AdomdCommand mdxCmd = new AdomdCommand(mdxCommandText, conn);
                AdomdDataReader mdxReader = mdxCmd.ExecuteReader(CommandBehavior.CloseConnection);

                while (mdxReader.Read())
                {
                    mdxTotals.Add("Imps", Convert.ToDouble(mdxReader[2]));
                    mdxTotals.Add("Clicks", Convert.ToDouble(mdxReader[3]));
                    mdxTotals.Add("Cost", Convert.ToDouble(mdxReader[4]));
                }
                mdxReader.Close();
            #endregion

                return IsEqual(Params, oltpTotals, mdxTotals, "Oltp", "Mdx");
            }
            catch ( Exception e )
            {
                return new ValidationResult()
                {
                    ResultType = ValidationResultType.Error,
                    AccountID = Convert.ToInt32(Params["AccountID"]),
                    Message = e.Message,
                    TargetPeriodStart = Convert.ToDateTime(Params["Date"]),
                    TargetPeriodEnd = Convert.ToDateTime(Params["Date"]),
                    ChannelID = Convert.ToInt32(Params["ChannelID"]),
                    CheckType = this.Instance.Configuration.Name
                };
            }


        }

       
    }
}
