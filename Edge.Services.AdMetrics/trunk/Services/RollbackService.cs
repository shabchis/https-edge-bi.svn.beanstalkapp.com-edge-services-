using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Core.Utilities;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;

namespace Edge.Services.AdMetrics
{
	public class RollbackService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			Edge.Core.Services.ServiceOutcome outcome = Core.Services.ServiceOutcome.Success;
			int roledBack = 0;
			if (this.Instance.Configuration.Options.ContainsKey("DeliveriesIDS"))
			{
				string[] deliveriesIds = this.Instance.Configuration.Options["DeliveriesIDS"].Split(',');

				foreach (string deliveryID in deliveriesIds)
				{
					using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(this,"OLTP")))
					{
						conn.Open();
						using (SqlCommand cmd = DataManager.CreateCommand("SP_Delivery_Rollback_By_DeliveryID(@DeliveryID:NvarChar,@TableName:NvarChar)", System.Data.CommandType.StoredProcedure))
						{
							cmd.Connection = conn;
							cmd.Parameters["@DeliveryID"].Value = deliveryID;
							cmd.Parameters["@TableName"].Value="Paid_API_AllColumns_v29";
							cmd.ExecuteNonQuery();
								roledBack++;
						}
					}
				}
				if (roledBack != deliveriesIds.Count())
				{
					outcome = Core.Services.ServiceOutcome.Failure;
					Log.Write(string.Format("from {0} only {1} roled back", deliveriesIds.Count(), roledBack), LogMessageType.Error);

				}






			}
			else
			{
				outcome = Core.Services.ServiceOutcome.Failure;
				Log.Write("the option 'DeliveriesIDS' could not be found", LogMessageType.Error);

			}
			return outcome;
		}


	}
}
