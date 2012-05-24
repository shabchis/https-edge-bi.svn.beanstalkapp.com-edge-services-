using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Core.Configuration;
using System.Data.SqlClient;
using Edge.Core.Data;

namespace Edge.Services.AdMetrics
{
	/*
	public class ValidateCommitService : ValidationService
	{
		protected override ValidationResult Validate()
		{ 
			DeliveryHistoryEntry rollbackEntry = null;
			ValidationResult result = new ValidationResult() { Success = true };
			if (this.Delivery.History.Count(entry => entry.Operation == DeliveryOperation.Imported) == 0)
				throw new Exception("The delivery has not been imported yet.");
			DeliveryHistoryEntry importEntry = this.Delivery.History.Last(entry => entry.Operation == DeliveryOperation.Imported);
			if (this.Delivery.History.Count(entry => entry.Operation == DeliveryOperation.Committed) ==0)
				throw new Exception("The delivery has not been committed yet.");
			DeliveryHistoryEntry commitEntry = this.Delivery.History.Last(entry => entry.Operation == DeliveryOperation.Committed);
			if (this.Delivery.History.Count(entry => entry.Operation == DeliveryOperation.RolledBack) > 0)
				rollbackEntry = this.Delivery.History.Last(entry => entry.Operation == DeliveryOperation.RolledBack);			

			// Was rolled back
			//if (rollbackEntry != null && this.Delivery.History.IndexOf(rollbackEntry) > this.Delivery.History.IndexOf(commitEntry))
			//    throw new InvalidOperationException("The delivery has been rolled back.");
			var totals = (Dictionary<string, double>)importEntry.Parameters["Totals"];
			
			string tableName = this.Instance.Configuration.Options[Const.ConfigurationOptions.TableName];
			StringBuilder cmd = new StringBuilder();
			
			using (SqlConnection sqlConnection = new SqlConnection(AppSettings.GetConnectionString(this, "OLTP")))
			{
				sqlConnection.Open();
				Dictionary<string, Measure> measures = Measure.GetMeasures(this.Delivery.Account, this.Delivery.Channel, sqlConnection, MeasureOptions.IntegrityCheckRequired, MeasureOptionsOperator.And);
				foreach (KeyValuePair<string, Measure> measure in measures)
				{
					cmd.AppendFormat("SUM({0}) AS '{1}',", measure.Value.OltpName, measure.Key);
				}
				if (cmd.Length > 1)
				{
					cmd = cmd.Remove(cmd.Length - 1, 1);
					cmd.Insert(0,"SELECT ");
					cmd.AppendFormat(" \nFROM {0}", tableName);
					cmd.Append("\n WHERE DeliveryID=@DeliveryID:Nvarchar");
					using (SqlCommand sqlCommand = DataManager.CreateCommand(cmd.ToString(),System.Data.CommandType.Text))
					{
						sqlCommand.Parameters["@DeliveryID"].Value = this.Delivery.DeliveryID.ToString("N");
						sqlCommand.Connection = sqlConnection;

						using (SqlDataReader reader = sqlCommand.ExecuteReader())
						{

							if (reader.Read())
							{
								foreach (KeyValuePair<string, double> total in totals)
								{
									if (Math.Round(total.Value,2) !=Math.Round(Convert.ToDouble(reader[total.Key]),2))
									{
										result.Success = false;
										if (result.Parameters == null)
											result.Parameters = new Dictionary<string, object>();
										result.Parameters[total.Key] = total.Value;
										result.Message += string.Format("total {0} in delivery({1}) not equal total {2} in OLTP({3})\n", total.Key, total.Value, total.Key, reader[total.Key]);
									}

								} 
							}


						}

					}
				}
				
			}



			return result;
		}
	}
	*/
}
