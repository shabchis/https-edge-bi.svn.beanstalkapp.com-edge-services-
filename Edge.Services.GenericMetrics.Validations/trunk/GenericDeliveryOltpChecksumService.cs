using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Objects;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Metrics.Checksums;
using Edge.Core.Utilities;

namespace Edge.Services.GenericMetrics.Validations
{
	class GenericDeliveryOltpChecksumService : DeliveryDBChecksumBaseService
	{
		protected override Data.Pipeline.Services.ValidationResult DeliveryDbCompare(Data.Pipeline.DeliveryOutput output, Dictionary<string, double> deliveryTotals, string DbConnectionStringName, string comparisonTable)
		{
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, DbConnectionStringName)))
			{
				sqlCon.Open();
				Dictionary<string, Measure> measurs = Measure.GetMeasures(output.Account, output.Channel, sqlCon, MeasureOptions.IsBackOffice, OptionsOperator.Or);
				string dayCode = output.TimePeriodStart.ToString("yyyyMMdd"); // Delivery Per Day = > TargetPeriod.Start = daycode
				Dictionary<string, double> diff = new Dictionary<string, double>();
				Dictionary<string, Measure> validationRequiredMeasure = new Dictionary<string, Measure>();

				//creating command
				StringBuilder command = new StringBuilder();
				command.Append("Select ");

				foreach (var measure in measurs)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						validationRequiredMeasure.Add(measure.Value.Name, measure.Value);
						command.Append(string.Format("SUM({0}) as [{1}]", measure.Value.OltpName, measure.Value.OltpName));
						command.Append(",");
					}
				}
				command.Remove(command.Length - 1, 1); //remove last comma character
				command.Append(" from ");
				command.Append(comparisonTable);
				command.Append(" where account_id = @Account_ID and Day_Code = @Daycode ");

				//Shay: In comment due to Workaround for bo channel issue 11.03.2012
				command.Append("and IsNull(Channel_ID,-1) = @Channel_ID ");

				SqlCommand sqlCommand = DataManager.CreateCommand(command.ToString());
				sqlCommand.Connection = sqlCon;

				//SQL Parameters
				SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
				SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);

				//Shay: In comment due to Workaround for bo channel issue 11.03.2012
				SqlParameter channelIdParam = new SqlParameter("@Channel_ID", System.Data.SqlDbType.Int);


				accountIdParam.Value = output.Account.ID;
				daycodeParam.Value = dayCode;

				//Shay: In comment due to Workaround for bo channel issue 11.03.2012
				channelIdParam.Value = output.Channel.ID;

				sqlCommand.Parameters.Add(accountIdParam);
				sqlCommand.Parameters.Add(daycodeParam);

				//Shay: In comment due to Workaround for bo channel issue 11.03.2012
				sqlCommand.Parameters.Add(channelIdParam);

				//Getting Totals form DB
				using (SqlDataReader _reader = sqlCommand.ExecuteReader())
				{
					progress += 0.5 / 1 - progress;
					this.ReportProgress(progress);

					if (!_reader.IsClosed)
					{
						while (_reader.Read())
						{
							//Data Exists in DB
							if (!_reader[0].Equals(DBNull.Value))
							{
								foreach (var measureItem in validationRequiredMeasure)
								{
									if (deliveryTotals.ContainsKey(measureItem.Value.Name))
									{
									diff.Add(measureItem.Value.OltpName,
										Math.Abs(Convert.ToDouble(_reader[measureItem.Value.OltpName] == DBNull.Value ? 0 : _reader[measureItem.Value.OltpName]) - deliveryTotals[measureItem.Value.Name]));
									}
									else
										Log.Write(string.Format("Measure Name {0} ,not find on checksums",measureItem.Value.Name),LogMessageType.Warning);
								}
							}

							#region Scenario : data exists in delivery and not in DB
							else // No Data in DB
							{
								double sum = 0;
								foreach (var measure in validationRequiredMeasure)
								{
									sum += deliveryTotals[measure.Value.Name];
								}
								if (sum != 0) //Totals in Delivery aren't empty
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
							#endregion
								#region Scenario: TotalSUM in Delivery and Total Sum in DB are empty

								else  //Totals in Delivery are empty
									return new ValidationResult()
									{
										ResultType = ValidationResultType.Information,
										AccountID = output.Account.ID,
										DeliveryID = output.DeliveryID,
										TargetPeriodStart = output.TimePeriodStart,
										TargetPeriodEnd = output.TimePeriodEnd,
										Message = "validation Success: ** NOTE: NO DATA IN BOTH DELIVERY AND DB !!! - Account ID: " + output.Account.ID,
										ChannelID = output.Channel.ID,
										CheckType = this.Instance.Configuration.Name
									};
								#endregion

							}

								

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
					return new ValidationResult()
						{
							ResultType = ValidationResultType.Error,
							AccountID = output.Account.ID,
							DeliveryID = output.DeliveryID,
							TargetPeriodStart = output.TimePeriodStart,
							TargetPeriodEnd = output.TimePeriodEnd,
							Message = "Cannot Read Data from DB connection closed - Account ID: " + output.Account.ID,
							ChannelID = output.Channel.ID,
							CheckType = this.Instance.Configuration.Name
						};
				}
			}
		}
	}
}
