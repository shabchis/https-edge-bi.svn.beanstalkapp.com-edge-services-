using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Data.SqlClient;
using Edge.Data.Pipeline;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Metrics.Checksums;


namespace Edge.Services.SegmentMetrics.Validations
{
	class DeliveryDwhCheckSumService : DeliveryDBChecksumBaseService
	{
		
		protected override ValidationResult DeliveryDbCompare(DeliveryOutput output, Dictionary<string, double> deliveryTotals, string DbConnectionStringName, string comparisonTable)
		{


			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, DbConnectionStringName)))
			{
				sqlCon.Open();
				Dictionary<string, Measure> measurs = Measure.GetMeasures(output.Account, output.Channel, sqlCon, MeasureOptions.IsBackOffice, OptionsOperator.And);
				string dayCode = output.TimePeriodStart.ToString("yyyyMMdd"); // Delivery Per Day = > TargetPeriod.Start = daycode
				Dictionary<string, double> diff = new Dictionary<string, double>();
				Dictionary<string, Measure> validationRequiredMeasure = new Dictionary<string, Measure>();

				
				#region Creating Command txt
				/*==========================================================*/
				StringBuilder command = new StringBuilder();
				command.Append("Select ");
				foreach (var measure in measurs)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						validationRequiredMeasure.Add(measure.Value.SourceName, measure.Value);
						command.Append(string.Format("SUM({0}) as {1}", measure.Value.OltpName, measure.Value.SourceName));
						command.Append(",");
					}
				}
				command.Remove(command.Length - 1, 1); //remove last comma character
				command.Append(" from ");
				command.Append(comparisonTable);
				command.Append(" where Account_ID = @Account_ID and Day_ID = @Daycode and Channel_ID = @Channel_ID");
				/*===========================================================*/
				#endregion

				#region SQL Command
				/*==============================================================================*/
				SqlCommand sqlCommand = DataManager.CreateCommand(command.ToString());
				sqlCommand.Connection = sqlCon;

				SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
				SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);
				SqlParameter channelIdParam = new SqlParameter("@Channel_ID", System.Data.SqlDbType.Int);

				accountIdParam.Value = output.Account.ID;
				daycodeParam.Value = dayCode;
				channelIdParam.Value = output.Channel.ID;

				sqlCommand.Parameters.Add(accountIdParam);
				sqlCommand.Parameters.Add(daycodeParam);
				sqlCommand.Parameters.Add(channelIdParam);

				sqlCommand.Connection = sqlCon;
				/*==============================================================================*/
				#endregion
				

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
								foreach (var measureItem in validationRequiredMeasure)
								{
									diff.Add(measureItem.Value.SourceName,
										Math.Abs(Convert.ToDouble(_reader[measureItem.Value.SourceName]) - deliveryTotals[measureItem.Value.SourceName]));
								}
							}

							#region Scenario : data exists in delivery and not in DB
							else
							{
								double sum = 0;
								foreach (var measure in validationRequiredMeasure)
								{
									sum += deliveryTotals[measure.Value.SourceName];
								}
								if (sum > 0)
									return new ValidationResult()
									{
										ResultType = ValidationResultType.Error,
										AccountID = output.Account.ID,
										TargetPeriodStart = output.TimePeriodStart,
										TargetPeriodEnd = output.TimePeriodEnd,
										Message = "Data exists in delivery but not in DB for Account ID: " + output.Account.ID,
										ChannelID = output.Channel.ID,
										CheckType = this.Instance.Configuration.Name
									};

							}

							#endregion

							foreach (var item in diff)
							{
								#region Scenario: Found differences
								if (item.Value > ALLOWED_DIFF)
									return new ValidationResult()
									{
										ResultType = ValidationResultType.Error,
										AccountID = output.Account.ID,
										DeliveryID = output.DeliveryID,
										TargetPeriodStart = output.TimePeriodStart,
										TargetPeriodEnd = output.TimePeriodEnd,
										Message = "validation Error - differences has been found - Account  ID: " + output.Account.ID,
										ChannelID = output.Channel.ID,
										CheckType = this.Instance.Configuration.Name
									};
								#endregion
								#region Scenario: Differences were not found
								else
									return new ValidationResult()
									{
										ResultType = ValidationResultType.Information,
										AccountID = output.Account.ID,
										DeliveryID = output.DeliveryID,
										TargetPeriodStart = output.TimePeriodStart,
										TargetPeriodEnd = output.TimePeriodEnd,
										Message = "validation Success - Account ID: " + output.Account.ID,
										ChannelID = output.Channel.ID,
										CheckType = this.Instance.Configuration.Name
									};
								#endregion
							}
						}

					}

					// If reader is closed
					else
						return new ValidationResult()
						{
							ResultType = ValidationResultType.Error,
							AccountID = output.Account.ID,
							DeliveryID = output.DeliveryID,
							TargetPeriodStart = output.TimePeriodStart,
							TargetPeriodEnd = output.TimePeriodEnd,
							Message = "Cannot Read Data from DB connection closed - Account Original ID: " + output.Account.OriginalID,
							ChannelID = output.Channel.ID,
							CheckType = this.Instance.Configuration.Name
						};
				}
				//Could not find check scenario 
				return new ValidationResult()
				{
					ResultType = ValidationResultType.Error,
					AccountID = output.Account.ID,
					DeliveryID = output.DeliveryID,
					TargetPeriodStart = output.TimePeriodStart,
					TargetPeriodEnd = output.TimePeriodEnd,
					Message = "Could not find check scenario - Account Original ID: " + output.Account.OriginalID,
					ChannelID = output.Channel.ID,
					CheckType = this.Instance.Configuration.Name
				};

			}
		}

		
	}
}
