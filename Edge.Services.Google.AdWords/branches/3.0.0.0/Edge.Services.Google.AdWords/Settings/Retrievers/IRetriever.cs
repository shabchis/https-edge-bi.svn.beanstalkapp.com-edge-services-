using System.Collections.Generic;
using Edge.Data.Pipeline;
using Google.Api.Ads.AdWords.Lib;

namespace Edge.Services.Google.AdWords.Settings.Retrievers
{
	public interface IRetriever
	{
		IEnumerable<string> RetrieveData(DeliveryFile file, AdWordsUser user);
	}
}
