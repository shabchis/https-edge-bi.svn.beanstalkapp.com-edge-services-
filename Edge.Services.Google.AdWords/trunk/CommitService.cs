using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.Google.Adwords
{
	class CommitService : AdDataCommitBase
	{
		public override DeliveryManager GetDeliveryManager()
		{
			return new GoogleSearchDeliveryManager();
		}
	}
}
