using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Core.Utilities;

namespace Edge.Services.AdMetrics
{
	public class RollbackService: PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			Edge.Core.Services.ServiceOutcome outcome=Core.Services.ServiceOutcome.Success;
			if (this.Instance.Configuration.Options.ContainsKey("DeliveriesIDS"))
			{
				string[] deliveriesIds = this.Instance.Configuration.Options["DeliveriesIDS"].Split(',');
				List<Delivery> deliverires = new List<Delivery>();
				foreach (string deliveryID in deliveriesIds)
				{

					Guid currentGuid;
					if (Guid.TryParse(deliveryID, out currentGuid))
					{
						try
						{
							Delivery delivery = Delivery.Get(currentGuid);
							if (delivery != null)
								deliverires.Add(delivery);
							else
								Log.Write(string.Format("Could not find delivery id {0}", currentGuid.ToString()), LogMessageType.Warning);

						}
						catch (Exception ex)
						{

							Log.Write(string.Format("Could not find delivery id {0}", currentGuid.ToString()), ex, LogMessageType.Warning);
						}
					}
				}
				AdMetricsImportManager adMetricsImportManager = new AdMetricsImportManager(this.Instance.InstanceID, new AdMetricsImportManager.ImportManagerOptions()
				{
					SqlRollbackCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlRollbackCommand]
				});
				if (deliverires.Count == 0)
				{
					Log.Write("No deliveries found", LogMessageType.Error);
					outcome = Core.Services.ServiceOutcome.Failure;
				}
				else
				{
					adMetricsImportManager.Rollback(deliverires.ToArray());
					outcome = Core.Services.ServiceOutcome.Success;
				}
			}
			else
			{
				outcome = Core.Services.ServiceOutcome.Failure;
				Log.Write("the option 'DeliveriesIDS' could not be found" , LogMessageType.Error);

			}
			return outcome;
		}

		
	}
}
