using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;

namespace Edge.Services.Facebook.AdsApi
{
	public class ProcessorService:PipelineService
	{
		protected override Core.Services.ServiceOutcome DoWork()
		{
			return Core.Services.ServiceOutcome.Success;
		}
	}
}
