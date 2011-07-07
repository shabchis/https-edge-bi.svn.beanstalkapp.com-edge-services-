using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Importing;

namespace Edge.Services.Facebook.AdsApi
{
	public class CommitService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			using (var manager = new AdMetricsImportManager(this.Delivery))
			{
				Delivery[] conflicts = this.Delivery.GetConflicting();
				if (conflicts.Length > 0)
					manager.Rollback(conflicts);
			}
		}
	}
}
