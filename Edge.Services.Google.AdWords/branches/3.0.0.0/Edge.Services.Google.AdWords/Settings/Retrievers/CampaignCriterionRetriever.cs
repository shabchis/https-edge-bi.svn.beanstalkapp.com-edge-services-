using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Edge.Data.Pipeline;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.v201302;

namespace Edge.Services.Google.AdWords.Settings.Retrievers
{
	/// <summary>
	/// Specific retriever for Google CampaignCriterionService - 
	/// retrieve data from the service and save it in TXT file with headers in the 1st row and row for each criterion with fields seperated by comma
	/// to be processed as regular performance report
	/// </summary>
	public class CampaignCriterionRetriever : IRetriever
	{
		#region Consts
		private const int PAGE_SIZE = 500; 
		#endregion

		#region IRetriever implementation
		IEnumerable<string> IRetriever.RetrieveData(DeliveryFile file, AdWordsUser user)
		{
			var offset = 0;
			var sb = new StringBuilder();
			// 1st row is a header (field list)
			var outputLines = new List<string> { file.Parameters["ReportFields"].ToString() };

			var service = user.GetService(AdWordsService.v201302.CampaignCriterionService) as CampaignCriterionService;
			if (service == null)
				throw new Exception("Failed to create CampaignCriterionService");

			var selector = new Selector
			{
				fields = file.Parameters["ReportFields"].ToString().Split(','),
				paging = new Paging { numberResults = PAGE_SIZE },
			};

			if (!String.IsNullOrEmpty(file.Parameters["ReportFilter"].ToString()))
				selector.predicates = new[] { new Predicate { field = "CriteriaType", @operator = PredicateOperator.IN, values = file.Parameters["ReportFilter"].ToString().Split('|') } };

			//var query = String.Format("SELECT {0} WHERE CriteriaType IN ('LOCATION', 'LANGUAGE') LIMIT {{0}}, {1} ", file.Parameters["ReportFields"], PAGE_SIZE);
			CampaignCriterionPage page;
			do
			{
				// get next page
				selector.paging.startIndex = offset;
				page = service.get(selector);

				if (page != null && page.entries != null)
				{
					// for each criterion create row with all fields
					foreach (var campaignCriterion in page.entries)
					{
						sb.Clear();
						foreach (var field in selector.fields)
						{
							sb.AppendFormat("{0}{1}", sb.Length > 0 ? "," : "", GetFieldValue(campaignCriterion, field));
						}
						outputLines.Add(sb.ToString());
					}
				}
				offset += PAGE_SIZE;
			} while (page != null && offset < page.totalNumEntries);

			return outputLines;
		} 
		#endregion

		#region Private Methods
		private string GetFieldValue(CampaignCriterion campaignCriterion, string fieldName)
		{
			return fieldName == "Id" ? campaignCriterion.criterion.id.ToString(CultureInfo.InvariantCulture) :
					fieldName == "CriteriaType" ? campaignCriterion.criterion.type.ToString() :
					fieldName == "IsNegative" ? campaignCriterion.isNegative.ToString() :
					fieldName == "CampaignId" ? campaignCriterion.campaignId.ToString(CultureInfo.InvariantCulture) :
					fieldName == "LanguageCode" ? campaignCriterion.criterion is Language ? (campaignCriterion.criterion as Language).code : "" :
					fieldName == "LanguageName" ? campaignCriterion.criterion is Language ? (campaignCriterion.criterion as Language).name : "" :
					fieldName == "LocationName" ? campaignCriterion.criterion is Location ? (campaignCriterion.criterion as Location).locationName : "" :
					fieldName == "DisplayType" ? campaignCriterion.criterion is Location ? (campaignCriterion.criterion as Location).displayType : "" :
					"";
		} 
		#endregion
	}
}
