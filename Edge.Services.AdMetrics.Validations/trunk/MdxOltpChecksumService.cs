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
using Edge.Data.Pipeline.Metrics.Checksums;
using Edgeobjects = Edge.Data.Objects;
using Edge.Core.Data;
using Edge.Data.Pipeline.Metrics;

namespace Edge.Services.AdMetrics.Validations
{
	class MdxOltpChecksumService : DbDbChecksumBaseService
	{

		protected override Data.Pipeline.Services.ValidationResult Compare(string SourceTable, string TargetTabel, Dictionary<string, string> Params)
		{
			//{"AccountID",account},
			//               {"ChannelID",channel},
			//               {"Date",fromDate.ToString()}
			Edgeobjects.Account account = new Edgeobjects.Account() { ID = int.Parse(Params["AccountID"]) };
			Edgeobjects.Channel channel = new Edgeobjects.Channel() { ID = int.Parse(Params["ChannelID"]) };
			Dictionary<string, Diffrence> diffs = new Dictionary<string, Diffrence>();
			Dictionary<string, Edgeobjects.Measure> measures;
			string checksumThreshold = Instance.Configuration.Options[Consts.ConfigurationOptions.ChecksumTheshold];
			double allowedChecksumThreshold = checksumThreshold == null ? 0.01 : double.Parse(checksumThreshold);
			
			Dictionary<string, double> oltpTotals = new Dictionary<string, double>();
			Dictionary<string, double> mdxTotals = new Dictionary<string, double>();

			string dayCode = Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd");

			#region Getting measuers from oltp

			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "OltpDB")))
			{
				sqlCon.Open();
				StringBuilder sqlBuilder = new StringBuilder(); ;
				measures = Edgeobjects.Measure.GetMeasures(account, channel, sqlCon, Edgeobjects.MeasureOptions.IsTarget | Edgeobjects.MeasureOptions.IsCalculated | Edgeobjects.MeasureOptions.IsBackOffice, Edgeobjects.OptionsOperator.Not);
				foreach (var measure in measures)
				{
					if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
						sqlBuilder.AppendFormat("{1}SUM([{0}]) as [{0}]", measure.Value.OltpName, sqlBuilder.Length > 0 ? ", " : null);

				}
				sqlBuilder.Insert(0, "SELECT \n");
				sqlBuilder.AppendFormat(" FROM {0}\n ", SourceTable);
				sqlBuilder.Append("WHERE Account_ID=@accountID:int \nAND Day_Code=@daycode:int \nAND  Channel_ID=@Channel_ID:int");
				

				SqlCommand sqlCommand = DataManager.CreateCommand(sqlBuilder.ToString(), CommandType.Text);
				sqlCommand.Parameters["@accountID"].Value = account.ID;
				sqlCommand.Parameters["@daycode"].Value = dayCode; ;
				sqlCommand.Parameters["@Channel_ID"].Value = channel.ID;
				
				sqlCommand.Connection = sqlCon;

				using (var _reader = sqlCommand.ExecuteReader())
				{
					progress += 0.5 * (1 - progress);
					this.ReportProgress(progress);

					if (!_reader.IsClosed)
					{
						if (_reader.Read())
						{
							foreach (var measure in measures)
							{
								Diffrence difference = new Diffrence();
								if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
								{
									if (!_reader[measure.Value.OltpName].Equals(DBNull.Value))
									{
										oltpTotals.Add(measure.Value.Name,_reader.Get<double>(measure.Value.OltpName));
										
									}
								}
							}
							//if (!_reader[0].Equals(DBNull.Value))
							//{
							//    oltpTotals.Add("Cost", Convert.ToDouble(_reader[0]));
							//    oltpTotals.Add("Imps", Convert.ToDouble(_reader[1]));
							//    oltpTotals.Add("Clicks", Convert.ToDouble(_reader[2]));
							//}
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
				string CubeName = GetCubeName(Convert.ToInt32(Params["AccountID"]));
				StringBuilder mdxBuilder = new StringBuilder();
				mdxBuilder.Append("{{ ");
				foreach (var measure in measures)
				{
					if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
						mdxBuilder.AppendFormat("[Measures.[{0}],", measure.Value.DisplayName);

				}
				mdxBuilder.Remove(mdxBuilder.Length - 1, 1); //remove the last ','
				mdxBuilder.AppendFormat(@"}}
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
                                ", account.ID, CubeName, channel.ID, Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd"));


				conn.Open();

				//TO DO : Get Cube Name from DB


				//                string mdxCommandText = string.Format(@"Select
				//                                {{ [Measures].[Impressions],[Measures].[Clicks],[Measures].[Cost]}}
				//                                    On Columns , 
				//                                (
				//	                            [Accounts Dim].[Accounts].[Account].&[{0}]
				//                                )On Rows 
				//                                From
				//                                [{1}]
				//                                WHERE
				//                                ([Channels Dim].[Channels].[Channel].&[{2}]
				//                                ,[Time Dim].[Time Dim].[Day].&[{3}]
				//                                ) 
				//                                ", Params["AccountID"], CubeName, Params["ChannelID"], Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd"));

				AdomdCommand mdxCmd = new AdomdCommand(mdxBuilder.ToString(), conn);
				
				using (AdomdDataReader mdxReader = mdxCmd.ExecuteReader(CommandBehavior.CloseConnection))
				{

					if (mdxReader.Read())
					{
						foreach (var measure in measures)
						{
							if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
							{
								mdxTotals.Add(measure.Value.Name,Convert.ToDouble(mdxReader[measure.Value.DisplayName]));
								


							}

						}
						//mdxTotals.Add("Imps", Convert.ToDouble(mdxReader[2]));
						//mdxTotals.Add("Clicks", Convert.ToDouble(mdxReader[3]));
						//mdxTotals.Add("Cost", Convert.ToDouble(mdxReader[4]));
					}
					mdxReader.Close(); 
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
