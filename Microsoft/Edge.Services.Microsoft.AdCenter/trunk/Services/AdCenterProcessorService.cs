using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using System.ServiceModel;
using Edge.Data.Pipeline;
using System.IO;
using System.IO.Compression;
using System.Xml;
using System.Reflection;
using GotDotNet.XPath;
using Edge.Core.Data;
using Edge.Data.Pipeline.GkManager;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;
using Edge.Core;


namespace Edge.Services.Microsoft.AdCenter
{
    public class AdCenterProcessorService : PipelineService
    {
        protected override ServiceOutcome DoWork()
        {
			// TODO: add checks for delivery state

			// Create the readers
			var keywordReportReader = (XPathRowReader<PpcDataUnit>) this.Delivery.Files["KeywordPerformance"].CreateReader();
			var adReportReader = (XPathRowReader<PpcDataUnit>)this.Delivery.Files["AdPerformance"].CreateReader();

			keywordReportReader.OnNextRowRequired = innerReader =>
			{
				SettingsCollection xmlValues = GetElementValues(innerReader);
				return UnitFromValues(xmlValues);
			};


			// Start a transaction
			try
			{
				using (DataManager.Current.OpenConnection())
				{
					DataManager.Current.StartTransaction();

					while (keywordReportReader.Read())
					{
					}

					DataManager.Current.CommitTransaction();
				}
			}
			finally
			{
				keywordReportReader.Dispose();
				adReportReader.Dispose();
			}

            return ServiceOutcome.Success;
        }

		private PpcDataUnit UnitFromValues(SettingsCollection values)
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
			string rawMatchType;
			MatchType matchType;
			if (values.TryGetValue(data.Extra.AdgroupKeyword_OriginalMatchType, out rawMatchType))
				Enum.TryParse<MatchType>(rawMatchType, out matchType);

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

			// currency conversion
			data.CurrencyID = Currency.GetByCode(data.Extra.Currency_Code).ID;

			//..................
			// METRICS

			data.Impressions			= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Impressions.ToString()]);
			data.Clicks					= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Clicks.ToString()]);
			data.Cost					= Double.Parse(values[WS.KeywordPerformanceReportColumn.Spend.ToString()]);
			data.AveragePosition		= Double.Parse(values[WS.KeywordPerformanceReportColumn.AveragePosition.ToString()]);
			data.Conversions			= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Conversions.ToString()]);

			return data;
		}

		private SettingsCollection GetElementValues(XmlReader reader)
		{
			var dict = new SettingsCollection();
			using (var r = reader.ReadSubtree())
			{
				while (r.Read())
				{
					if (r.NodeType == XmlNodeType.Element)
						dict[r.Name] = r.ReadElementContentAsString();
				}
			}
			return dict;
		}
    }
}
