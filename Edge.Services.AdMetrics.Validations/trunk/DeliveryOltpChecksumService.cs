﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Data.Pipeline.Metrics.Checksums;
using Edge.Data.Pipeline.Metrics;


namespace Edge.Services.AdMetrics.Validations
{

	public class DeliveryOltpChecksumService : DeliveryDBChecksumBaseService
	{
		protected override ValidationResult DeliveryDbCompare(DeliveryOutput deliveryOutput, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable)
		{
			double costDif = 0;
			double impsDif = 0;
			double clicksDif = 0;
			string dayCode = deliveryOutput.TimePeriodStart.ToString("yyyyMMdd"); // Delivery Per Day = > TargetPeriod.Start = daycode
			Dictionary<string, Diffrence> diffs = new Dictionary<string, Diffrence>();
			string checksumThreshold = Instance.Configuration.Options[Consts.ConfigurationOptions.ChecksumTheshold];
			double allowedChecksumThreshold = checksumThreshold == null ? 0.01 : double.Parse(checksumThreshold);
			ValidationResult result;




			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, DbConnectionStringName)))
			{
				sqlCon.Open();
				//Get measures need to be check
				StringBuilder sqlBuilder = new StringBuilder(); ;
				Dictionary<string, Measure> measures = Measure.GetMeasures(deliveryOutput.Account, deliveryOutput.Channel, sqlCon, MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice, OptionsOperator.Not);
				foreach (var measure in measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
						sqlBuilder.AppendFormat("{1}SUM([{0}]) as [{0}]", measure.Value.OltpName, sqlBuilder.Length > 0 ? ", " : null);

				}
				sqlBuilder.Insert(0, "SELECT \n");
				sqlBuilder.AppendFormat(" FROM {0}\n ",comparisonTable);
				sqlBuilder.Append("WHERE Account_ID=@accountID:int \nAND Day_Code=@daycode:int \nAND  Channel_ID=@Channel_ID:int \nand Account_ID_SRC=@OriginalID:nvarchar");


				SqlCommand sqlCommand = DataManager.CreateCommand(sqlBuilder.ToString());
				// "SELECT SUM(cost),sum(imps),sum(clicks) from " + comparisonTable +
				//" where account_id = @Account_ID and Day_Code = @Daycode and Channel_ID = @Channel_ID" +
				//" and [Account_ID_SRC] = @OriginalID"
				//);

				sqlCommand.Parameters["@accountID"].Value = deliveryOutput.Account.ID;
				sqlCommand.Parameters["@daycode"].Value = dayCode;;
				sqlCommand.Parameters["@Channel_ID"].Value = deliveryOutput.Channel.ID;
				sqlCommand.Parameters["@OriginalID"].Value=deliveryOutput.Account.OriginalID;
				//SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
				//SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);
				//SqlParameter channelIdParam = new SqlParameter("@Channel_ID", System.Data.SqlDbType.Int);
				//SqlParameter originalIdParam = new SqlParameter("@OriginalID", System.Data.SqlDbType.NVarChar);

				//accountIdParam.Value = deliveryOutput.Account.ID;
				//daycodeParam.Value = dayCode;
				//channelIdParam.Value = deliveryOutput.Channel.ID;
				//originalIdParam.Value = deliveryOutput.Account.OriginalID;

				//sqlCommand.Parameters.Add(accountIdParam);
				//sqlCommand.Parameters.Add(daycodeParam);
				//sqlCommand.Parameters.Add(channelIdParam);
				//sqlCommand.Parameters.Add(originalIdParam);

				sqlCommand.Connection = sqlCon;

				using (var _reader = sqlCommand.ExecuteReader())
				{


					if (!_reader.IsClosed)
					{
						if (_reader.Read())
						{
							foreach (var measure in measures)
							{
								Diffrence difference = new Diffrence();
								if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
								{
									if (!_reader[measure.Value.OltpName].Equals(DBNull.Value))
									{
										difference.Source = totals[measure.Value.Name];
										difference.Target = Convert.ToDouble(_reader[measure.Value.OltpName]);
										difference.diff = Math.Abs((Convert.ToDouble(_reader[measure.Value.OltpName]) - totals[measure.Value.Name]));
										diffs[measure.Value.Name] = difference;
									}
									#region Scenario : data exists in delivery and not in DB
									else if (totals[measure.Value.Name] > 0)
									{
										result = new ValidationResult()
										{
											ResultType = ValidationResultType.Error,
											AccountID = deliveryOutput.Account.ID,
											DeliveryID=deliveryOutput.DeliveryID,
											TargetPeriodStart = deliveryOutput.TimePeriodStart,
											TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
											Message =string.Format("Data exists in delivery but not in DB for Account {0} output {1}",deliveryOutput.Account.ID , deliveryOutput.OutputID.ToString("N")),
											ChannelID = deliveryOutput.Channel.ID,
											CheckType = this.Instance.Configuration.Name
										};
									}
									else //diff is 0
									{
										difference.Source = 0;
										difference.Target = 0;
										difference.diff = 0;
										diffs[measure.Value.Name] = difference;
									}
									#endregion





									//#region Scenario: Found differences

									//if ((costDif != 0 && (costDif / totals["Cost"] > ALLOWED_DIFF)) ||
									//(clicksDif != 0 && (clicksDif / totals["Clicks"] > ALLOWED_DIFF)) ||
									//(impsDif != 0 && (impsDif / totals["Impressions"] > ALLOWED_DIFF)))

									//    return new ValidationResult()
									//    {
									//        ResultType = ValidationResultType.Error,
									//        AccountID = deliveryOutput.Account.ID,
									//        DeliveryID = deliveryOutput.Delivery.DeliveryID,
									//        TargetPeriodStart = deliveryOutput.TimePeriodStart,
									//        TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
									//        Message = "validation Error - differences has been found - Account Original ID: " + deliveryOutput.Account.OriginalID,
									//        ChannelID = deliveryOutput.Channel.ID,
									//        CheckType = this.Instance.Configuration.Name
									//    };
									//#endregion

									//#region Scenario: Differences were not found
									//else
									//    return new ValidationResult()
									//    {
									//        ResultType = ValidationResultType.Information,
									//        AccountID = deliveryOutput.Account.ID,
									//        DeliveryID = deliveryOutput.Delivery.DeliveryID,
									//        TargetPeriodStart = deliveryOutput.TimePeriodStart,
									//        TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
									//        Message = "validation Success - Account Original ID: " + deliveryOutput.Account.OriginalID,
									//        ChannelID = deliveryOutput.Channel.ID,
									//        CheckType = this.Instance.Configuration.Name
									//    };
									//#endregion
								}
							}
							StringBuilder errors = new StringBuilder();
							foreach (KeyValuePair<string, Diffrence> diff in diffs)
							{
								if (diff.Value.diff / diff.Value.Source > allowedChecksumThreshold)
								{
									errors.AppendFormat("validation Error - differences has been found for Account-{0}-channel{1}-output{2}-measure{3} source{4} target {5}\n", deliveryOutput.Account.ID, deliveryOutput.Channel.ID, deliveryOutput.OutputID.ToString("N"), diff.Key, diff.Value.Source, diff.Value.Target); //TODO: USE 
								}
							}
							if (errors.Length > 0)
							{
								result = new ValidationResult()
								{
									ResultType = ValidationResultType.Error,
									AccountID = deliveryOutput.Account.ID,
									DeliveryID = deliveryOutput.DeliveryID,
									TargetPeriodStart = deliveryOutput.TimePeriodStart,
									TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
									Message = errors.ToString(),
									ChannelID = deliveryOutput.Channel.ID,
									CheckType = this.Instance.Configuration.Name

								};
							}
							else
							{
								return new ValidationResult()
										{
											ResultType = ValidationResultType.Information,
											AccountID = deliveryOutput.Account.ID,
											DeliveryID = deliveryOutput.DeliveryID,
											TargetPeriodStart = deliveryOutput.TimePeriodStart,
											TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
											Message = "validation Success - Account Original ID: " + deliveryOutput.Account.OriginalID,
											ChannelID = deliveryOutput.Channel.ID,
											CheckType = this.Instance.Configuration.Name
										};

							}



						}

					}

					// If reader is closed
					else
						result = new ValidationResult()
						{
							ResultType = ValidationResultType.Error,
							AccountID = deliveryOutput.Account.ID,
							DeliveryID = deliveryOutput.DeliveryID,
							TargetPeriodStart = deliveryOutput.TimePeriodStart,
							TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
							Message = "Cannot Read Data from DB connection closed - Account Original ID: " + deliveryOutput.Account.OriginalID,
							ChannelID = deliveryOutput.Channel.ID,
							CheckType = this.Instance.Configuration.Name
						};
				}
				//Could not find check scenario 
				result = new ValidationResult()
				{
					ResultType = ValidationResultType.Error,
					AccountID = deliveryOutput.Account.ID,
					DeliveryID = deliveryOutput.DeliveryID,
					TargetPeriodStart = deliveryOutput.TimePeriodStart,
					TargetPeriodEnd = deliveryOutput.TimePeriodEnd,
					Message = "Could not find check scenario - Account Original ID: " + deliveryOutput.Account.OriginalID,
					ChannelID = deliveryOutput.Channel.ID,
					CheckType = this.Instance.Configuration.Name
				};

			}
			return result;
		}
	}
	public class Diffrence
	{
		public double Target { get; set; }
		public double Source { get; set; }
		public double diff { get; set; }
	}



}
