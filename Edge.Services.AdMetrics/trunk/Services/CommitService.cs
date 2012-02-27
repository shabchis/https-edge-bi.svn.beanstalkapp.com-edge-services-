using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Common.Importing;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.AdMetrics
{
	public class CommitService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			string validationThreshold = Instance.Configuration.Options[Consts.AppSettings.CommitValidationTheshold];

			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new ImportManagerOptions()
			{
				SqlPrepareCommand = Instance.Configuration.Options[Consts.AppSettings.SqlPrepareCommand],
				SqlCommitCommand = Instance.Configuration.Options[Consts.AppSettings.SqlCommitCommand],
				SqlRollbackCommand = Instance.Configuration.Options[Consts.AppSettings.SqlRollbackCommand],
				CommitValidationThreshold = validationThreshold == null ? 0.01 : double.Parse(validationThreshold)
			});
			ReportProgress(0.1);


			// ----------------
			// TICKETS

			// Only check tickets, don't check conflicts
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Ignore, getBehaviorFromConfiguration: false);
			ReportProgress(0.2);

			// ----------------
			// PREPARE
			//for debug locks on edge 888
			try
			{
				Edge.Core.Utilities.Log.Write(string.Format("{0} Start Prepare", DateTime.Now.ToString("dd-MM-yyyy HH:mm")), Core.Utilities.LogMessageType.Information);
			}
			catch (Exception)
			{


			}

			importManager.Prepare(new Delivery[] { this.Delivery });


			try
			{
				Edge.Core.Utilities.Log.Write(string.Format("{0} End Prepare", DateTime.Now.ToString("dd-MM-yyyy HH:mm")), Core.Utilities.LogMessageType.Information);
			}
			catch (Exception)
			{


			}
			ReportProgress(0.6);

			// ----------------
			// COMMIT
			bool success = false;
			do
			{
				try
				{
					try
					{
						Edge.Core.Utilities.Log.Write(string.Format("{0} Start Commit", DateTime.Now.ToString("dd-MM-yyyy HH:mm")), Core.Utilities.LogMessageType.Information);
					}
					catch (Exception)
					{


					}
					importManager.Commit(new Delivery[] { this.Delivery });
					try
					{
						Edge.Core.Utilities.Log.Write(string.Format("{0} End Commit", DateTime.Now.ToString("dd-MM-yyyy HH:mm")), Core.Utilities.LogMessageType.Information);
					}
					catch (Exception)
					{


					}
					success = true;
				}
				catch (DeliveryConflictException dceex)
				{
					try
					{
						Edge.Core.Utilities.Log.Write(string.Format("{0} Start Rollback", DateTime.Now.ToString("dd-MM-yyyy HH:mm")), Core.Utilities.LogMessageType.Information);
					}
					catch (Exception)
					{


					}
					importManager.Rollback(dceex.ConflictingDeliveries);

					try
					{
						Edge.Core.Utilities.Log.Write(string.Format("{0} End Rollback", DateTime.Now.ToString("dd-MM-yyyy HH:mm")), Core.Utilities.LogMessageType.Information);
					}
					catch (Exception)
					{


					}
				}
				catch (Exception ex)
				{
					throw new Exception(String.Format("Delivery {0} failed during Commit.", this.Delivery.DeliveryID), ex);
				}
			}
			while (!success);


			///////////////////////////////////////////////////


			return ServiceOutcome.Success;
		}
	}
}
