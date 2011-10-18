using System;
using System.Collections.Generic;
using Edge.Core.Data;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Services.AdMetrics;
using Edge.Data.Pipeline.Services;
using WS = Edge.Services.Microsoft.AdCenter.AdCenter.Reporting;
using System.Text.RegularExpressions;


namespace Edge.Services.Microsoft.AdCenter
{
	public class ProcessorService : PipelineService
	{
		public const string endOfFileMicrosoftCorporation = "©2011 Microsoft Corporation. All rights reserved. ";
		static class MeasureNames
		{
			public const string AdCenterConversions = "AdCenterConversions";
		}
        static ExtraField AdType = new ExtraField() { ColumnIndex = 2, Name = "adType" };
        static Dictionary<string, int> AdCenterAdTypeDic;
		Dictionary<string, Campaign> _campaignsCache;
		Dictionary<long, Ad> _adCache;

        public ProcessorService()
        {

            AdCenterAdTypeDic = new Dictionary<string, int>()
			{
				{"Text ad",1},
				{"RichAd",2},
				{"Image",3}, // Not supported by AdCenter
				{"Local",4},// Not supported by AdCenter
				{"Mobile",5},
				{"ThirdPartyCreative",6}
			};
        }
		protected override ServiceOutcome DoPipelineWork()
		{

            // TODO: add checks for delivery state
			string[] requiredHeaders;
			requiredHeaders = new string[1];
			requiredHeaders[0] = WS.CampaignPerformanceReportColumn.CampaignName.ToString();

			_campaignsCache = new Dictionary<string, Campaign>();
			_adCache = new Dictionary<long, Ad>();
			DeliveryFile adReport = this.Delivery.Files[Const.Files.AdReport];
			DeliveryFile campaignReport = this.Delivery.Files[Const.Files.CampaignReport];
            FileInfo campaignReportFileInfo = campaignReport.GetFileInfo(ArchiveType.Zip);
            string[] campaignReportSubFiles = campaignReportFileInfo.GetSubFiles();

            var campaignReportReader = new CsvDynamicReader(
                campaignReport.OpenContents(subLocation: campaignReportSubFiles[0],archiveType: ArchiveType.Zip),
                requiredHeaders
                );
            
            #region Reading campaigns file
            while (campaignReportReader.Read())
            {
                string accountNameColVal = campaignReportReader.Current[WS.CampaignPerformanceReportColumn.AccountName.ToString()];

                if (accountNameColVal.Trim() == string.Empty || accountNameColVal.Trim() == endOfFileMicrosoftCorporation.Trim())//end of file
                    break;
                Campaign campaign = CreateCampaign(campaignReportReader.Current);
                _campaignsCache.Add(campaign.Name, campaign);
            }
            this.ReportProgress(0.2); 
            #endregion

            #region Read the ad report, and build a lookup table for later
            using (var session = new AdMetricsImportManager(this.Instance.InstanceID))
            {
                session.BeginImport(this.Delivery);
              
                // create the ad report reader
                requiredHeaders = new string[1];
                requiredHeaders[0] = WS.AdPerformanceReportColumn.AdId.ToString();

                string[] adReportSubFiles = adReport.GetFileInfo(ArchiveType.Zip).GetSubFiles();

                var adReportReader = new CsvDynamicReader(
                    adReport.OpenContents(subLocation: adReportSubFiles[0], archiveType: ArchiveType.Zip),
                    requiredHeaders);
                
                //read
                using (adReportReader)
                {

                    while (adReportReader.Read())
                    {
                        // create the ad
                        string accountNameColVal = adReportReader.Current[WS.AdPerformanceReportColumn.AccountName.ToString()];//end of file
                        if (accountNameColVal.Trim() == string.Empty || accountNameColVal.Trim() == endOfFileMicrosoftCorporation.Trim())
                            break;
                        Ad ad = CreateAd(adReportReader.Current);

                        ad.Campaign = _campaignsCache[ad.Campaign.Name];

                        // Adgroup
                        ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
                        {
                            Value = adReportReader.Current[WS.AdPerformanceReportColumn.AdGroupName.ToString()],
                            OriginalID = adReportReader.Current[WS.AdPerformanceReportColumn.AdGroupId.ToString()]
                        };

                        //Ad Type
                        string adTypeKey = Convert.ToString(adReportReader.Current[WS.AdPerformanceReportColumn.AdType.ToString()]);
                        ad.ExtraFields[AdType] = AdCenterAdTypeDic[adTypeKey];
                        
                        session.ImportAd(ad);

                        _adCache.Add(long.Parse(ad.OriginalID), ad);

                    }


                }
                this.ReportProgress(0.7);
                adReport.History.Add(DeliveryOperation.Imported, Instance.InstanceID); 
            #endregion

				// ...............................................................
                #region Read the keyword report, cross reference it with the ad data, and commit
                // Read the keyword report, cross reference it with the ad data, and commit

                // The name of the time period column is specified by the initializer, depending on the report
                DeliveryFile keywordReport = this.Delivery.Files[Const.Files.KeywordReport];
                string timePeriodColumn = keywordReport.Parameters[Const.Parameters.TimePeriodColumnName] as string;

                //    // create the keyword report reader
                requiredHeaders = new string[1];
                requiredHeaders[0] = "Keyword";

                string[] keywordReportSubFiles = keywordReport.GetFileInfo(ArchiveType.Zip).GetSubFiles();

                var keywordReportReader = new CsvDynamicReader(
                    keywordReport.OpenContents(subLocation: keywordReportSubFiles[0], archiveType: ArchiveType.Zip),
                    requiredHeaders);

                Dictionary<string, double> _totals = new Dictionary<string, double>(); //for checksum validation
                //Added by Shay for validation 
                foreach (KeyValuePair<string, Measure> measure in session.Measures)
                {
                    if (measure.Value.Options.HasFlag(MeasureOptions.IntegrityCheckRequired))
                    {
                        _totals.Add(measure.Key, 0);
                    }
                }

                // read and save in transaction
                using (keywordReportReader)
                {
                    while (keywordReportReader.Read())
                    {
                        string GregorianDateColVal = keywordReportReader.Current.GregorianDate;

                        if (GregorianDateColVal.Trim() == string.Empty || GregorianDateColVal.Trim() == endOfFileMicrosoftCorporation.Trim())//end of file
                            break;
                        // get the unit from the keyword report, and add the missing ad data
                        AdMetricsUnit unit = CreateMetrics(keywordReportReader.Current, timePeriodColumn, session);
                        session.ImportMetrics(unit);

                        // Validation information:
                        _totals[Measure.Common.Clicks] += unit.MeasureValues[session.Measures[Measure.Common.Clicks]];
                        _totals[Measure.Common.Cost] += unit.MeasureValues[session.Measures[Measure.Common.Cost]];
                        _totals[Measure.Common.Impressions] += unit.MeasureValues[session.Measures[Measure.Common.Impressions]];


                    }

                    session.HistoryEntryParameters.Add(AdMetricsImportManager.Consts.DeliveryHistoryParameters.ChecksumTotals, _totals);
                    session.EndImport();

                    ReportProgress(1);
                } 
                #endregion

			}

			return ServiceOutcome.Success;
		}

		private Campaign CreateCampaign(dynamic values)
		{
			Campaign campaign = new Campaign()
			{
				Account = new Account() { ID = this.Delivery.Account.ID, OriginalID = this.Delivery.Account.OriginalID },
				OriginalID = values[WS.CampaignPerformanceReportColumn.CampaignId.ToString()],
				Name = values[WS.CampaignPerformanceReportColumn.CampaignName.ToString()],
				Channel = new Channel() { ID = this.Delivery.Channel.ID }
			};
			string status = values[WS.CampaignPerformanceReportColumn.Status.ToString()];
			//switch (status)
			//{
					
			//    case "Active":
			//    case "Submitted":
			//        campaign.Status = CampaignStatus.Active;
			//        break;
			//    case "BudgetPaused":
			//        campaign.Status = CampaignStatus.Suspended;
			//        break;
			//    case "Paused":
			//        campaign.Status = CampaignStatus.Paused;
			//        break;
			//    case "Cancelled":
			//    case "Deleted":
			//        campaign.Status = CampaignStatus.Deleted;
			//        break;

			//}
			return campaign;
		}

        private Ad CreateAd(dynamic values)
		{
			Ad ad = new Ad()
			{
				OriginalID = values[WS.AdPerformanceReportColumn.AdId.ToString()],
				Campaign = new Campaign()
				{
					
					Name = values[WS.AdPerformanceReportColumn.CampaignName.ToString()]
				},
				Creatives = new List<Creative>()
				{
					new TextCreative(){ TextType = TextCreativeType.Title, Text = values[WS.AdPerformanceReportColumn.AdTitle.ToString()] },
					new TextCreative(){ TextType = TextCreativeType.Body, Text = values[WS.AdPerformanceReportColumn.AdDescription.ToString()] }
					
				},
				DestinationUrl=values[WS.AdPerformanceReportColumn.DestinationUrl.ToString()],
				
			};
			SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, ad.DestinationUrl);
			if (tracker != null)
				ad.Segments[Segment.TrackerSegment] = tracker;
			return ad;
		}

		private AdMetricsUnit CreateMetrics(dynamic values, string timePeriodColumn, AdMetricsImportManager session)
		{
			//if (String.IsNullOrWhiteSpace(timePeriodColumn))
			//    throw new ArgumentNullException("timePeriodColumn");
           
          

			var unit = new AdMetricsUnit();

			unit.Ad = _adCache[Convert.ToInt64(values[WS.KeywordPerformanceReportColumn.AdId.ToString()])];
			unit.Currency = new Currency() { Code = values[WS.KeywordPerformanceReportColumn.CurrencyCode.ToString()] };
			unit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
			unit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();
			unit.MeasureValues[session.Measures[Measure.Common.Clicks]] =double.Parse( values[WS.KeywordPerformanceReportColumn.Clicks.ToString()]);
			unit.MeasureValues[session.Measures[Measure.Common.Cost]] =double.Parse( values[WS.KeywordPerformanceReportColumn.Spend.ToString()]);
            unit.MeasureValues[session.Measures[Measure.Common.Impressions]] = double.Parse(values[WS.KeywordPerformanceReportColumn.Impressions.ToString()]);

			
			unit.MeasureValues[session.Measures[Measure.Common.AveragePosition]] =double.Parse( values[WS.KeywordPerformanceReportColumn.AveragePosition.ToString()]);
			unit.MeasureValues[session.Measures[MeasureNames.AdCenterConversions]] = double.Parse(values[WS.KeywordPerformanceReportColumn.Conversions.ToString()]);
			
			unit.TargetMatches = new List<Target>();
			KeywordTarget target = new KeywordTarget();
			target.DestinationUrl = values[WS.KeywordPerformanceReportColumn.DestinationUrl.ToString()];
			target.Keyword = values[WS.KeywordPerformanceReportColumn.Keyword.ToString()];
			string macthType = values[WS.KeywordPerformanceReportColumn.MatchType.ToString()];
			switch (macthType)
			{
				case "Exact":
					target.MatchType = KeywordMatchType.Exact;
					break;
				case "Broad":
					target.MatchType = KeywordMatchType.Broad;
					break;
				case "Phrase":
					target.MatchType = KeywordMatchType.Phrase;
					break;
				default:
					target.MatchType = KeywordMatchType.Unidentified;
					break;

			}
			target.OriginalID = values[WS.KeywordPerformanceReportColumn.KeywordId.ToString()];

			unit.TargetMatches.Add(target);

           

			return unit;
		}
	}

}
