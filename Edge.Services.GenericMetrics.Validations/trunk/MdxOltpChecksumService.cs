using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Data.Objects;
using Microsoft.AnalysisServices.AdomdClient;
using System.Data;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Metrics.Checksums;

namespace Edge.Services.GenericMetrics.Validations
{
    class MdxOltpChecksumService : DbDbChecksumBaseService
    {

        protected override Data.Pipeline.Services.ValidationResult Compare(string OltpTable, string DwhTabel, Dictionary<string, string> Params)
        {
            try
            {
                Dictionary<string, double> oltpTotals = new Dictionary<string, double>();
                Dictionary<string, double> mdxTotals = new Dictionary<string, double>();
                Dictionary<string, Edge.Data.Objects.Measure> validationRequiredMeasure = new Dictionary<string, Edge.Data.Objects.Measure>();

                #region Getting measuers from oltp

                using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "OltpDB")))
                {
                    sqlCon.Open();
                    Dictionary<string, Edge.Data.Objects.Measure> measurs = Edge.Data.Objects.Measure.GetMeasures(new Account() { ID = Convert.ToInt32(Params["AccountID"]) },
                        new Channel() { ID = Convert.ToInt32(Params["ChannelID"]) }, sqlCon, MeasureOptions.IsBackOffice, OptionsOperator.And);

                    string dayCode = Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd");
                    Dictionary<string, double> diff = new Dictionary<string, double>();


                    //creating command
                    StringBuilder command = new StringBuilder();
                    command.Append("Select ");

                    foreach (var measure in measurs)
                    {
                        if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
                        {
                            validationRequiredMeasure.Add(measure.Value.SourceName, measure.Value);
                            command.Append(string.Format("SUM({0}) as {1}", measure.Value.OltpName, measure.Value.OltpName));
                            command.Append(",");
                        }
                    }
                    command.Remove(command.Length - 1, 1); //remove last comma character
                    command.Append(" from ");
                    command.Append(OltpTable);
                    command.Append(" where account_id = @Account_ID and Day_Code = @Daycode and IsNull(Channel_ID,-1) = @Channel_ID");

                    SqlCommand sqlCommand = new SqlCommand(command.ToString());

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
                                    foreach (var measureItem in validationRequiredMeasure)
                                    {
                                        oltpTotals.Add(measureItem.Value.Name, _reader[measureItem.Value.OltpName] == DBNull.Value ? 0 : Convert.ToDouble(_reader[measureItem.Value.OltpName]));
                                    }
                                }
                            }

                        }

                    }
                }
                #endregion

                #region Getting measures from Analysis server (MDX)

                StringBuilder withMdxBuilder = new StringBuilder();
                StringBuilder selectMdxBuilder = new StringBuilder();
                StringBuilder fromMdxBuilder = new StringBuilder();


                //Getting CubeName from Database
                string CubeName = GetCubeName(Convert.ToInt32(Params["AccountID"]), false);

                //Creating MDX
                selectMdxBuilder.Append("Select {{");
                foreach (var requierdMeasure in validationRequiredMeasure)
                {
                    if (string.IsNullOrEmpty(requierdMeasure.Value.DisplayName))
                        return new ValidationResult()
                        {
                            ResultType = ValidationResultType.Error,
                            AccountID = Convert.ToInt32(Params["AccountID"]),
                            Message = string.Format("Measure Display Name cannot be NULL or Empty (Check Measure #{0})", requierdMeasure.Value.ID),
                            TargetPeriodStart = Convert.ToDateTime(Params["Date"]),
                            TargetPeriodEnd = Convert.ToDateTime(Params["Date"]),
                            ChannelID = Convert.ToInt32(Params["ChannelID"]),
                            CheckType = this.Instance.Configuration.Name
                        };

                    selectMdxBuilder.Append("[Measures].[" + requierdMeasure.Value.DisplayName + "],");
                }

                selectMdxBuilder.Remove(selectMdxBuilder.Length - 1, 1); //remove last comma character
                fromMdxBuilder.Append("}}");
                fromMdxBuilder.Append(

                    string.Format(@" On Columns ,
									(
									[Accounts Dim].[Accounts].[Account].&[{0}]
									)On Rows 
									From
									[{1}]
									WHERE
									([Time Dim].[Time Dim].[Day].&[{2}]
									) 
									", Params["AccountID"], CubeName, Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd"))
                    );


                #region Creating Command
                SqlCommand mdx_command = new SqlCommand("dbo.SP_ExecuteMDX");
                mdx_command.CommandType = CommandType.StoredProcedure;
                SqlParameter withMDX = new SqlParameter("WithMDX", withMdxBuilder.ToString());
                mdx_command.Parameters.Add(withMDX);

                SqlParameter selectMDX = new SqlParameter("SelectMDX", selectMdxBuilder.ToString());
                mdx_command.Parameters.Add(selectMDX);

                SqlParameter fromMDX = new SqlParameter("FromMDX", fromMdxBuilder.ToString());
                mdx_command.Parameters.Add(fromMDX);
                #endregion

                using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "OltpDB")))
                {
                    sqlCon.Open();
                    mdx_command.Connection = sqlCon;
                    using (var reader = mdx_command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            foreach (var measureItem in validationRequiredMeasure)
                            {
                                mdxTotals.Add(measureItem.Value.Name, reader[string.Format("[Measures].[{0}]", measureItem.Value.DisplayName)] == DBNull.Value ? 0 : Convert.ToDouble(reader[string.Format("[Measures].[{0}]", measureItem.Value.DisplayName)]));
                            }
                        }
                    }
                    if (mdxTotals.Count == 0)
                    {
                        foreach (var measure in validationRequiredMeasure)
                        {
                            mdxTotals.Add(measure.Value.Name, 0);
                        }

                    }
                }
                #endregion

                return IsEqual(Params, oltpTotals, mdxTotals, "Oltp", "Mdx");
            }

            catch (Exception e)
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
