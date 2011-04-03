using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using System.Text;
using System.Xml;
using Edge.Core;
using Edge.Core.Data;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.GkManager;
using Edge.Data.Pipeline.Readers;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;


namespace Edge.Services.Microsoft.AdCenter
{
    public class AdCenterProcessorService : PipelineService
    {
        protected override ServiceOutcome DoWork()
        {
			// TODO: add checks for delivery state

			// ...............................................................
			// Read the ad report, and build a lookup table for later

			DeliveryFile adReport = this.Delivery.Files["AdPerformance"];

			// create the ad report reader
			var adReportReader = new XmlChunkReader
			(
				adReport.SavedPath,
				Instance.Configuration.Options["AdCenter.AdPerformance.XPath"], // ./Report/Table/Row
				XmlChunkReaderOptions.ElementsAsValues
			);

			// lookup table by ad ID
			var ads = new Dictionary<string,AdData>();

			// read
			using (adReportReader)
			{
				while (adReportReader.Read())
				{
					var ad = new AdData()
					{
						AdId = adReportReader.Current[WS.AdPerformanceReportColumn.AdId.ToString()],
						AdTitle = adReportReader.Current[WS.AdPerformanceReportColumn.AdTitle.ToString()],
						AdDescription = adReportReader.Current[WS.AdPerformanceReportColumn.AdDescription.ToString()]
					};
					ads.Add(ad.AdId, ad);
				}
			}

			// mark the delivery file as processed
			adReport.History.Add(DeliveryOperation.Processed, Instance.InstanceID);

			// ...............................................................
			// Read the keyword report, cross reference it with the ad data, and commit

			DeliveryFile keywordReport = this.Delivery.Files["KeywordPerformance"];

			// create the keyword report reader
			var keywordReportReader = new XmlChunkReader
			(
				keywordReport.SavedPath,
				Instance.Configuration.Options["AdCenter.KeywordPerformance.XPath"], // ./Report/Table/Row
				XmlChunkReaderOptions.ElementsAsValues
			);

			// read and save in transaction
			using (keywordReportReader)
			{
				using (DataManager.Current.OpenConnection())
				{
					DataManager.Current.StartTransaction();

					while (keywordReportReader.Read())
					{
						// get the unit from the keyword report, and add the missing ad data
						PpcDataUnit data = CreateUnitFromKeywordReportChunk(keywordReportReader.Current);

						// get the matching ad
						AdData ad;
						if (!ads.TryGetValue(data.Extra.Creative_OriginalID, out ad))
						{
							// TODO: add to error log this data because there is no matching ad
							continue;
						}

						// legacy values
						data.Extra.Creative_Title = ad.AdTitle;
						data.Extra.Creative_Desc1 = ad.AdDescription;

						// GKs
						data.CreativeGK = GkManager.GetCreativeGK(Instance.AccountID, ad.AdTitle, ad.AdDescription, string.Empty);
					}

					DataManager.Current.CommitTransaction();
				}
			}

            return ServiceOutcome.Success;
        }

		private PpcDataUnit CreateUnitFromKeywordReportChunk(XmlChunk values)
		{
			var data = new PpcDataUnit();

			//..................
			// IDENTITIES

			// AccountID
			data.AccountID = Instance.AccountID;

			// Raw values (legacy DB schema)
			data.Extra.Account_OriginalID				= values[WS.KeywordPerformanceReportColumn.AccountNumber.ToString()];
			data.Extra.Campaign_Name					= values[WS.KeywordPerformanceReportColumn.CampaignName.ToString()];
			data.Extra.Campaign_OriginalID				= values[WS.KeywordPerformanceReportColumn.CampaignId.ToString()];
			data.Extra.Adgroup_Name						= values[WS.KeywordPerformanceReportColumn.AdGroupName.ToString()];
			data.Extra.Adgroup_OriginalID				= values[WS.KeywordPerformanceReportColumn.AdGroupId.ToString()];
			data.Extra.Keyword_Text						= values[WS.KeywordPerformanceReportColumn.Keyword.ToString()];
			data.Extra.Keyword_OriginalID				= values[WS.KeywordPerformanceReportColumn.KeywordId.ToString()];
			data.Extra.Creative_OriginalID				= values[WS.KeywordPerformanceReportColumn.AdId.ToString()];
			data.Extra.AdgroupKeyword_DestUrl			= values[WS.KeywordPerformanceReportColumn.DestinationUrl.ToString()];
			data.Extra.AdgroupKeyword_OriginalMatchType	= values[WS.KeywordPerformanceReportColumn.MatchType.ToString()];
			data.Extra.Currency_Code					= values[WS.KeywordPerformanceReportColumn.CurrencyCode.ToString()];

			// Match type
			MatchType matchType = MatchType.Unidentified;
			if (data.Extra.AdgroupKeyword_OriginalMatchType != null)
				Enum.TryParse<MatchType>(data.Extra.AdgroupKeyword_OriginalMatchType, out matchType);

			// campaign
			data.CampaignGK = GkManager.GetCampaignGK(
				Instance.AccountID,
				Delivery.ChannelID,
				data.Extra.Campaign_Name,
				data.Extra.Campaign_OriginalID
				);

			// adgroup
			data.AdgroupGK = GkManager.GetAdgroupGK(
				Instance.AccountID,
				Delivery.ChannelID,
				data.CampaignGK.Value,
				data.Extra.Adgroup_Name,
				data.Extra.Adgroup_OriginalID
				);

			// keyword
			data.KeywordGK = GkManager.GetKeywordGK(
				Instance.AccountID,
				data.Extra.Keyword_Text
				);

			// adgroup keyword
			data.AdgroupKeywordGK = GkManager.GetAdgroupKeywordGK(
				Instance.AccountID,
				Delivery.ChannelID,
				data.CampaignGK.Value,
				data.AdgroupGK.Value,
				data.KeywordGK.Value,
				matchType,
				data.Extra.AdgroupKeyword_DestUrl,
				null
				);

			// TODO: currency conversion data
			// data.CurrencyID = Currency.GetByCode(data.Extra.Currency_Code).ID;

			//..................
			// METRICS

			data.Impressions			= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Impressions.ToString()]);
			data.Clicks					= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Clicks.ToString()]);
			data.Cost					= Double.Parse(values[WS.KeywordPerformanceReportColumn.Spend.ToString()]);
			data.AveragePosition		= Double.Parse(values[WS.KeywordPerformanceReportColumn.AveragePosition.ToString()]);
			data.Conversions			= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Conversions.ToString()]);

			return data;
		}
    }

	internal class AdData
	{
		public string AdId;
		public string AdTitle;
		public string AdDescription;
	}
}
