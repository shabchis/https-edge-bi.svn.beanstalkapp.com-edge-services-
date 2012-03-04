using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Objects;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Services.Common.Validation;

namespace Edge.Services.SegmentMetrics.Validations
{
	class SegmentDeliveryOltpChecksumService : DeliveryDBChecksumBaseService
	{
		protected override Data.Pipeline.Services.ValidationResult DeliveryDbCompare(Data.Pipeline.Delivery delivery, Dictionary<string, double> deliveryTotals, string DbConnectionStringName, string comparisonTable)
		{
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, DbConnectionStringName)))
			{
				sqlCon.Open();
				Dictionary<string, Measure> measurs = Measure.GetMeasures(delivery.Account, delivery.Channel, sqlCon, MeasureOptions.IsBackOffice, MeasureOptionsOperator.And);
				string dayCode = delivery.TargetPeriodStart.ToString("yyyyMMdd"); // Delivery Per Day = > TargetPeriod.Start = daycode
				Dictionary<string, double> diff = new Dictionary<string, double>();
				Dictionary<string,Measure> validationRequiredMeasure = new Dictionary<string,Measure>();

				//creating command
				StringBuilder command = new StringBuilder();
				command.Append("Select ");

				foreach (var measure in measurs)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						validationRequiredMeasure.Add(measure.Value.SourceName,measure.Value);
						command.Append(string.Format("SUM({0}) as {1}", measure.Value.OltpName, measure.Value.SourceName));
						command.Append(",");
					}
				}
				command.Remove(command.Length - 1, 1); //remove last comma character
				command.Append(" from ");
				command.Append(comparisonTable);
				command.Append(" where account_id = @Account_ID and Day_Code = @Daycode");

				SqlCommand sqlCommand = DataManager.CreateCommand(command.ToString());
				sqlCommand.Connection = sqlCon;
				SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
				SqlParameter daycodeParam = new SqlParameter("@Daycode", System.Data.SqlDbType.Int);

				accountIdParam.Value = delivery.Account.ID;
				daycodeParam.Value = dayCode;

				sqlCommand.Parameters.Add(accountIdParam);
				sqlCommand.Parameters.Add(daycodeParam);

				using (SqlDataReader _reader = sqlCommand.ExecuteReader())
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
										AccountID = delivery.Account.ID,
										TargetPeriodStart = delivery.TargetPeriodStart,
										TargetPeriodEnd = delivery.TargetPeriodEnd,
										Message = "Data exists in delivery but not in DB for Account ID: " + delivery.Account.ID,
										ChannelID = delivery.Channel.ID,
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
													   AccountID = delivery.Account.ID,
													   DeliveryID = delivery.DeliveryID,
													   TargetPeriodStart = delivery.TargetPeriodStart,
													   TargetPeriodEnd = delivery.TargetPeriodEnd,
													   Message = "validation Error - differences has been found - Account  ID: " + delivery.Account.ID,
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
										Message = "validation Success - Account ID: " + delivery.Account.ID,
										ChannelID = delivery.Channel.ID,
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
							AccountID = delivery.Account.ID,
							DeliveryID = delivery.DeliveryID,
							TargetPeriodStart = delivery.TargetPeriodStart,
							TargetPeriodEnd = delivery.TargetPeriodEnd,
							Message = "Cannot Read Data from DB connection closed - Account ID: " + delivery.Account.ID,
							ChannelID = delivery.Channel.ID,
							CheckType = this.Instance.Configuration.Name
						};
				}
			}
		}
	}
}
