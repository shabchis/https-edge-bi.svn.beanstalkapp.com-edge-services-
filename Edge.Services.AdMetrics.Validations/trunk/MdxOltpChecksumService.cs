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
using Edge.Core.Utilities;

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
										oltpTotals.Add(measure.Value.Name, Convert.ToDouble(_reader[measure.Value.OltpName]));

									}
								}
							}
						}

					}

				}
			}
			#endregion
			//bool boool=true;
			//return Debug(Params, measures, oltpTotals, mdxTotals, boool);

			#region Getting measures from Analysis server (MDX)

			string admobConnection = this.Instance.Configuration.Options["AdmobConnection"];
			AdomdConnection conn = new AdomdConnection(admobConnection);
			StringBuilder mdxBuilder = new StringBuilder();
			try
			{
				string cubeName = string.Empty;
				bool isGDN = false;
				if (!String.IsNullOrEmpty(this.Instance.Configuration.Options["GDNCubeName"]))
					isGDN = Convert.ToBoolean(this.Instance.Configuration.Options["GDNCubeName"]);
			
				cubeName = GetCubeName(Convert.ToInt32(Params["AccountID"]),isGDN);
				
				mdxBuilder.Append("SELECT {{ ");
				foreach (var measure in measures)
				{
					if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
					{
						string cubeMeasureName = string.Empty;
						
						//checking for GDN cube 
						if (isGDN)
							cubeMeasureName = string.Format("Content {0}", measure.Value.DisplayName);
						else
							cubeMeasureName = measure.Value.DisplayName;

						mdxBuilder.AppendFormat("[Measures].[{0}],", cubeMeasureName);
					}

				}
				mdxBuilder.Remove(mdxBuilder.Length - 1, 1); //remove the last ','
				mdxBuilder.Append("}}\n");
				mdxBuilder.AppendFormat(@"On Columns , 
                                (
	                            [Accounts Dim].[Accounts].[Account].&[{0}]
                                )On Rows 
                                From
                                [{1}]
                                WHERE
                                ([Channels Dim].[Channels].[Channel].&[{2}]
                                ,[Time Dim].[Time Dim].[Day].&[{3}]
                                ) 
                                ", account.ID, cubeName, channel.ID, Convert.ToDateTime(Params["Date"]).ToString("yyyyMMdd"));
				conn.Open();
				AdomdCommand mdxCmd = new AdomdCommand(mdxBuilder.ToString(), conn);
				using (AdomdDataReader mdxReader = mdxCmd.ExecuteReader(CommandBehavior.CloseConnection))
				{
					if (!mdxReader.IsClosed)
					{

						if (mdxReader.Read())
						{
							foreach (var measure in measures)
							{
								if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
								{
									mdxTotals.Add(measure.Value.Name, mdxReader[string.Format("[Measures].[{0}]", measure.Value.DisplayName)] == DBNull.Value ? 0 : Convert.ToDouble(mdxReader[string.Format("[Measures].[{0}]", measure.Value.DisplayName)]));
								}
							}

						}
					}
					else
					{
						foreach (var measure in measures)
						{
							if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
							{
								mdxTotals.Add(measure.Value.Name, 0);
							}
						}

					}

				}

				return IsEqual(Params, oltpTotals, mdxTotals, "Oltp", "Mdx");

			}
			catch (Exception e)
			{
				Log.Write(string.Format("exception while runing MDX - {0} ", mdxBuilder.ToString()), e, LogMessageType.Error);
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

			#endregion
		}

		private ValidationResult Debug(Dictionary<string, string> Params, Dictionary<string, Edgeobjects.Measure> measures, Dictionary<string, double> oltpTotals, Dictionary<string, double> mdxTotals, bool boool)
		{
			try
			{

				if (boool)
				{
					foreach (var measure in measures)
					{
						if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
							mdxTotals.Add(measure.Value.Name, 9999);

					}
				}
				else
				{
					foreach (var measure in measures)
					{
						if (measure.Value.Options.HasFlag(Edgeobjects.MeasureOptions.ValidationRequired))
							mdxTotals.Add(measure.Value.Name, 0);

					}

				}
				return IsEqual(Params, oltpTotals, mdxTotals, "Oltp", "Mdx");



			}
			catch (Exception e)
			{

				Log.Write("exception", e, LogMessageType.Error);
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
