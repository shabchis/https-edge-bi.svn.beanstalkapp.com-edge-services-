using System;
using System.Collections.Generic;
using Edge.Core.Data;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using WS = Edge.Services.Microsoft.AdCenter.AdCenter.Reporting;
using System.Text.RegularExpressions;
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Common.Importing;
using System.Linq;

namespace Edge.Services.Microsoft.AdCenter
{
	public class ProcessorService : MetricsProcessorServiceBase
	{
        DeliveryOutput currentOutput;
		public const string endOfFileMicrosoftCorporation = "©2011 Microsoft Corporation. All rights reserved. ";
		static class MeasureNames
		{
			public const string AdCenterConversions = "AdCenterConversions";
		}
		static ExtraField AdType = new ExtraField() { ColumnIndex = 2, Name = "adType" };
		static Dictionary<string, int> AdCenterAdTypeDic;
		Dictionary<string, Campaign> _campaignsCache;
		Dictionary<long, Ad> _adCache;
	
		public new AdMetricsImportManager ImportManager
		{
			get { return (AdMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}

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
			currentOutput = Delivery.Outputs.First();
			currentOutput.Checksum=new Dictionary<string,double>();
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
				campaignReport.OpenContents(subLocation: campaignReportSubFiles[0], archiveType: ArchiveType.Zip),
				requiredHeaders
				);

			#region Reading campaigns file
			while (campaignReportReader.Read())
			{
				string accountNameColVal = campaignReportReader.Current[WS.CampaignPerformanceReportColumn.AccountName.ToString()];

				if (accountNameColVal.Trim() == string.Empty || accountNameColVal.Trim().Contains(endOfFileMicrosoftCorporation.Trim()))//end of file
					break;
				Campaign campaign = CreateCampaign(campaignReportReader.Current);
				_campaignsCache.Add(campaign.Name, campaign);
			}
			this.ReportProgress(0.2);
			#endregion

			#region Read the ad report, and build a lookup table for later
			using (this.ImportManager = new AdMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
			{
				MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
				MeasureOptionsOperator = OptionsOperator.Not,
				SegmentOptions = Data.Objects.SegmentOptions.All,
				SegmentOptionsOperator = OptionsOperator.And
			}))
			{
				ImportManager.BeginImport(this.Delivery);



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
						this.Mappings.OnFieldRequired = field => adReportReader.Current[field];
						string accountNameColVal = adReportReader.Current[WS.AdPerformanceReportColumn.AccountName.ToString()];//end of file
						if (accountNameColVal.Trim() == string.Empty || accountNameColVal.Trim() == endOfFileMicrosoftCorporation.Trim())
							break;

						Ad ad = CreateAd(adReportReader.Current, ImportManager);
						this.Mappings.Objects[typeof(Ad)].Apply(ad);
						
						//ADDING CAMPAIGN
						ad.Segments[this.ImportManager.
							SegmentTypes[Segment.Common.Campaign]] =_campaignsCache[adReportReader.Current[WS.AdPerformanceReportColumn.CampaignName.ToString()]];

						

						//CREATING Adgroup
						AdGroup adGroup = new AdGroup()
						{
							Value = adReportReader.Current[WS.AdPerformanceReportColumn.AdGroupName.ToString()],
							Campaign = (Campaign)ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]],
							OriginalID = adReportReader.Current[WS.AdPerformanceReportColumn.AdGroupId.ToString()]
						};
						ad.Segments.Add(ImportManager.SegmentTypes[Segment.Common.AdGroup], adGroup);

						//CRAETING AD TYPE
						string adTypeKey = Convert.ToString(adReportReader.Current[WS.AdPerformanceReportColumn.AdType.ToString()]);
						ad.ExtraFields[AdType] = AdCenterAdTypeDic[adTypeKey];

						ImportManager.ImportAd(ad);

						_adCache.Add(long.Parse(ad.OriginalID), ad);

					}


				}
				this.ReportProgress(0.7);
				
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

				
				//Added by Shay for validation 
				foreach (KeyValuePair<string, Measure> measure in ImportManager.Measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						currentOutput.Checksum.Add(measure.Key, 0);
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
						AdMetricsUnit unit = CreateMetrics(keywordReportReader.Current, timePeriodColumn, ImportManager);
						ImportManager.ImportMetrics(unit);

						// Validation information:
						currentOutput.Checksum[Measure.Common.Clicks] += unit.MeasureValues[ImportManager.Measures[Measure.Common.Clicks]];
						currentOutput.Checksum[Measure.Common.Cost] += unit.MeasureValues[ImportManager.Measures[Measure.Common.Cost]];
						currentOutput.Checksum[Measure.Common.Impressions] += unit.MeasureValues[ImportManager.Measures[Measure.Common.Impressions]];

					}

				
					ImportManager.EndImport();

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
				OriginalID = values[WS.CampaignPerformanceReportColumn.CampaignId.ToString()],
				Name = values[WS.CampaignPerformanceReportColumn.CampaignName.ToString()],
			};
			string status = values[WS.CampaignPerformanceReportColumn.Status.ToString()];

			#region Status
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
			#endregion

			return campaign;
		}

		private Ad CreateAd(dynamic values, AdMetricsImportManager session)
		{
			Ad ad = new Ad()
			{
				OriginalID = values[WS.AdPerformanceReportColumn.AdId.ToString()],
				Creatives = new List<Creative>()
				{
					new TextCreative(){ TextType = TextCreativeType.Title, Text = values[WS.AdPerformanceReportColumn.AdTitle.ToString()] },
					new TextCreative(){ TextType = TextCreativeType.Body, Text = values[WS.AdPerformanceReportColumn.AdDescription.ToString()] },
                    new TextCreative(){ TextType = TextCreativeType.DisplayUrl, Text = string.Empty }
					
				},
				DestinationUrl = values[WS.AdPerformanceReportColumn.DestinationUrl.ToString()],

				Account = new Account() { ID = this.Delivery.Account.ID, OriginalID = this.Delivery.Account.OriginalID },
				Channel = new Channel() { ID = this.Delivery.Channel.ID }
			};
			
			return ad;
		}

		private AdMetricsUnit CreateMetrics(dynamic values, string timePeriodColumn, AdMetricsImportManager session)
		{
			var metricsUnit = new AdMetricsUnit();
            metricsUnit.Output = currentOutput;
            metricsUnit.MeasureValues = new Dictionary<Measure, double>();
			metricsUnit.Ad = _adCache[Convert.ToInt64(values[WS.KeywordPerformanceReportColumn.AdId.ToString()])];
			metricsUnit.Currency = new Currency() { Code = values[WS.KeywordPerformanceReportColumn.CurrencyCode.ToString()] };
			metricsUnit.TimePeriodStart = this.Delivery.TimePeriodDefinition.Start.ToDateTime();
			metricsUnit.TimePeriodEnd = this.Delivery.TimePeriodDefinition.End.ToDateTime();
			metricsUnit.MeasureValues[session.Measures[Measure.Common.Clicks]] = double.Parse(values[WS.KeywordPerformanceReportColumn.Clicks.ToString()]);
			metricsUnit.MeasureValues[session.Measures[Measure.Common.Cost]] = double.Parse(values[WS.KeywordPerformanceReportColumn.Spend.ToString()]);
			metricsUnit.MeasureValues[session.Measures[Measure.Common.Impressions]] = double.Parse(values[WS.KeywordPerformanceReportColumn.Impressions.ToString()]);


			metricsUnit.MeasureValues[session.Measures[Measure.Common.AveragePosition]] = double.Parse(values[WS.KeywordPerformanceReportColumn.AveragePosition.ToString()]);
			metricsUnit.MeasureValues[session.Measures[MeasureNames.AdCenterConversions]] = double.Parse(values[WS.KeywordPerformanceReportColumn.Conversions.ToString()]);


			metricsUnit.TargetDimensions = new List<Target>();

			//CREATING KEYWORD TARGET
			KeywordTarget kwdTarget = new KeywordTarget();
			kwdTarget.DestinationUrl = values[WS.KeywordPerformanceReportColumn.DestinationUrl.ToString()];
			kwdTarget.Keyword = values[WS.KeywordPerformanceReportColumn.Keyword.ToString()];
			string macthType = values[WS.KeywordPerformanceReportColumn.MatchType.ToString()];
			switch (macthType)
			{
				case "Exact":
					kwdTarget.MatchType = KeywordMatchType.Exact;
					break;
				case "Broad":
					kwdTarget.MatchType = KeywordMatchType.Broad;
					break;
				case "Phrase":
					kwdTarget.MatchType = KeywordMatchType.Phrase;
					break;
				default:
					kwdTarget.MatchType = KeywordMatchType.Unidentified;
					break;

			}
			kwdTarget.OriginalID = values[WS.KeywordPerformanceReportColumn.KeywordId.ToString()];
			metricsUnit.TargetDimensions.Add(kwdTarget);

			return metricsUnit;
		}
	}

}
