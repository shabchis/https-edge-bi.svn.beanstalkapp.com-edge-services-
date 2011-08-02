using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.AdMetrics
{
	public class CommitService: PipelineService
	{ 
		protected override ServiceOutcome DoPipelineWork()
		{
			string validationThreshold = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.CommitValidationTheshold];

			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new AdMetricsImportManager.ImportManagerOptions()
			{
				SqlPrepareCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlPrepareCommand],
				SqlCommitCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlCommitCommand],
				SqlRollbackCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlRollbackCommand],
				CommitValidationThreshold = validationThreshold == null ? 0.01 : double.Parse(validationThreshold)
			});
			ReportProgress(0.1);

			DeliveryRollbackOperation rollback = this.HandleConflicts(importManager, DeliveryConflictBehavior.Rollback,
				getBehaviorFromConfiguration:false
			);
			ReportProgress(0.2);
			if (rollback != null)
				rollback.Wait();
			ReportProgress(0.3);

			importManager.Commit(new Delivery[] { this.Delivery });

			return ServiceOutcome.Success;
		}
	}
}
