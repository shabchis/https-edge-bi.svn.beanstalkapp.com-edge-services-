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
	public class CommitService: PipelineService
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
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Ignore, getBehaviorFromConfiguration:false);
			ReportProgress(0.2);

			// ----------------
			// PREPARE
			importManager.Prepare(new Delivery[] { this.Delivery });
			ReportProgress(0.6);

			// ----------------
			// COMMIT
			bool success = false;
			do
			{
				try
				{
					
					importManager.Commit(new Delivery[] { this.Delivery });
					success = true;
				}
				catch (DeliveryConflictException dceex)
				{
					importManager.Rollback(dceex.ConflictingDeliveries);
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
