using System.Collections.Generic;
using Edge.Data.Pipeline;
using Google.Api.Ads.AdWords.Lib;

namespace Edge.Services.Google.AdWords.Settings.Retrievers
{
	/// <summary>
	/// Interface for retrieving data from Google services
	/// </summary>
	public interface IRetriever
	{
		/// <summary>
		/// Retrieve data from specific Google Adwords service 
		/// </summary>
		/// <param name="file">delivery file to get parameters for retrieval</param>
		/// <param name="user">adwords user</param>
		/// <returns>list of rows per each retrieved object</returns>
		IEnumerable<string> RetrieveData(DeliveryFile file, AdWordsUser user);
	}
}
