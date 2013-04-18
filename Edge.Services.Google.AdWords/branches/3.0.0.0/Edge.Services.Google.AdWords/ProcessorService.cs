using System;
using System.Collections.Generic;
using Edge.Core.Services;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Misc;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Objects;
using GA = Google.Api.Ads.AdWords.v201302;
using Edge.Data.Pipeline.Metrics.Services;
using System.Linq;

namespace Edge.Services.Google.AdWords
{
	public class ProcessorService : MetricsProcessorServiceBase
    {
		#region Edge Ad types
		public enum EdgeAdType
		{
			Text_ad = 1,
			Flash = 2,
			Image_ad = 3,
			Display_ad = 4,
			Product_listing_ad = 5,
			Mobile_ad = 6,
			Local_business_ad = 7,
			Third_party_ad = 8,
			Other = 9,
			Mobile_text = 10,
			Mobile_image = 11,
			Mobile_display = 12
		} 
		#endregion
		
		#region Data Members
		static Dictionary<string, EdgeAdType> _googleAdTypeDic;
		static Dictionary<string, string> _googleMeasuresDic;
		static Dictionary<string, ObjectStatus> _objectStatusDic; 
		#endregion

		#region Ctor
		public ProcessorService()
		{
			_googleAdTypeDic = new Dictionary<string, EdgeAdType>
	            {
				{AdWordsConst.AdTypeValues.Text_ad,EdgeAdType.Text_ad},
				{AdWordsConst.AdTypeValues.Flash_ad,EdgeAdType.Flash},
				{AdWordsConst.AdTypeValues.Image_ad,EdgeAdType.Image_ad},
				{AdWordsConst.AdTypeValues.Display_ad,EdgeAdType.Display_ad},
				{AdWordsConst.AdTypeValues.Product_listing_ad,EdgeAdType.Product_listing_ad},
				{AdWordsConst.AdTypeValues.Mobile_ad,EdgeAdType.Mobile_ad},
				{AdWordsConst.AdTypeValues.Local_business_ad,EdgeAdType.Local_business_ad},
				{AdWordsConst.AdTypeValues.Third_party_ad,EdgeAdType.Third_party_ad},
				{AdWordsConst.AdTypeValues.Other,EdgeAdType.Other},
                {AdWordsConst.AdTypeValues.Mobile_text,EdgeAdType.Mobile_text},
                {AdWordsConst.AdTypeValues.Mobile_image,EdgeAdType.Mobile_image},
                {AdWordsConst.AdTypeValues.Mobile_display,EdgeAdType.Mobile_display}
			};

			_googleMeasuresDic = new Dictionary<string, string>
	            {
				{"Lead","Leads"},
				{"Signup","Signups"},
				{"Purchase","Purchases"},
				{"Purchase/Sale","Purchases"},
				{"Pageview","PageViews"},
				{"Default","Default"},
				{AdWordsConst.ConversionOnePerClickFieldName,"TotalConversionsOnePerClick"},
				{AdWordsConst.ConversionManyPerClickFieldName,"TotalConversionsManyPerClick"}
			};

			_objectStatusDic = new Dictionary<string, ObjectStatus>
	            {
				{"PAUSED",ObjectStatus.Paused},
				{"DISABLED",ObjectStatus.Paused},
				{"DELETED",ObjectStatus.Deleted},
				{"ACTIVE",ObjectStatus.Active},
				{"ENABLED",ObjectStatus.Active}
			};
		} 
		#endregion

		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			InitMappings();

			bool includeConversionTypes = Boolean.Parse(Delivery.Parameters["includeConversionTypes"].ToString());
			bool includeDisplaytData = Boolean.Parse(Delivery.Parameters["includeDisplaytData"].ToString());

			//Status Members
			var kwd_Status_Data = new Dictionary<string, ObjectStatus>();
			var placement_kwd_Status_Data = new Dictionary<string, ObjectStatus>();
			var adGroup_Status_Data = new Dictionary<Int64, ObjectStatus>();
			var ad_Status_Data = new Dictionary<Int64, ObjectStatus>();
			var campaign_Status_Data = new Dictionary<Int64, ObjectStatus>();

			using (ImportManager = new MetricsDeliveryManager(InstanceID, EdgeTypes, new MetricsDeliveryManagerOptions()))
			//{

			//	MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
			//	MeasureOptionsOperator = OptionsOperator.Not,
			//	SegmentOptions = Data.Objects.SegmentOptions.All,
			//	SegmentOptionsOperator = OptionsOperator.And
			//}))
			{
				var requiredHeaders = new string[1];
				requiredHeaders[0] = AdWordsConst.AdPreRequiredHeader;
				var totals = new Dictionary<string, double>();

				// Getting Keywords Data
				var keywordsCache = LoadKeywords(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT]], requiredHeaders);

				// Getting Placements Data
				var placementsCache = LoadPlacements(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT]], requiredHeaders);

				// Getting Conversions Data ( for ex. signup , purchase )
				var importedAdsWithConv = LoadConversions(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv"], requiredHeaders);

				#region Getting Ads Data

				var adPerformanceFile = Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT]];
				var adsReader = new CsvDynamicReader(adPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
				var importedAds = new Dictionary<string, Ad>();

				//session.Begin(false);
				ImportManager.BeginImport(Delivery, GetSampleMetrics());
				var currentOutput = Delivery.Outputs.First();

				//foreach (KeyValuePair<string, Measure> measure in this.ImportManager.Measures)
				//{
				//	if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
				//	{
				//		totals.Add(measure.Key, 0);
				//	}
				//}

				using (adsReader)
				{
					Mappings.OnFieldRequired = field => adsReader.Current[field];

					while (adsReader.Read())
					{
						// Adding totals line for validation (checksum)
						//if (adsReader.Current[AdWordsConst.AdIDFieldName] == AdWordsConst.EOF)
						//{
						//	foreach (KeyValuePair<string, Measure> measure in this.ImportManager.Measures)
						//	{
						//		if (!measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
						//			continue;

						//		switch (measure.Key)
						//		{
						//			case Measure.Common.Clicks: totals[Measure.Common.Clicks] = Convert.ToInt64(adsReader.Current.Clicks); break;
						//			case Measure.Common.Cost: totals[Measure.Common.Cost] = (Convert.ToDouble(adsReader.Current.Cost)) / 1000000; break;
						//			case Measure.Common.Impressions: totals[Measure.Common.Impressions] = Convert.ToInt64(adsReader.Current.Impressions); break;
						//		}
						//	}
						//	break;
						//}

						//adMetricsUnit.Output = currentOutput;
						var metricsUnit = new MetricsUnit();
						Ad ad;

						string adId = adsReader.Current[AdWordsConst.AdIDFieldName];
						if (importedAds.ContainsKey(adId))
						{
							ad = importedAds[adId];
						}
						else
						{
							ad = new Ad
							{
								OriginalID = adId,
								Channel = new Channel { ID = 1 },
								Account = new Account { ID = Delivery.Account.ID }
							};
							// ad.Status = ad_Status_Data[Convert.ToInt64(adId)];

							//--------------
							// Ad Type
							//--------------
							string adTypeColumnValue = Convert.ToString(adsReader.Current[AdWordsConst.AdTypeFieldName]);
							string devicePreferenceColumnValue = Convert.ToString(adsReader.Current[AdWordsConst.AdDevicePreferenceFieldName]);
							string adTypeEdgeValue = _googleAdTypeDic[adTypeColumnValue].ToString();

							//EdgeAdType atv = (EdgeAdType)Enum.Parse(typeof(EdgeAdType), adTypeEdgeValue, true);

							//is mobile ad ? 
							if (devicePreferenceColumnValue.Equals(AdWordsConst.AdDevicePreferenceMobileFieldValue))
							{
								string mobileValue = string.Format("Mobile {0}", Convert.ToString(adsReader.Current[AdWordsConst.AdTypeFieldName]));

								//Check if this mobile value exists on dictionary
								adTypeEdgeValue = _googleAdTypeDic.ContainsKey(mobileValue) ? _googleAdTypeDic[mobileValue].ToString() : _googleAdTypeDic[AdWordsConst.AdTypeValues.Mobile_ad].ToString();
							}
							ad.Fields.Add(GetExtraField("AdType"), (int)(EdgeAdType)Enum.Parse(typeof(EdgeAdType), adTypeEdgeValue, true));

							//------------------
							// Destination Url
							//------------------
							if (!String.IsNullOrWhiteSpace(adsReader.Current[AdWordsConst.DestUrlFieldName]))
								ad.DestinationUrl = adsReader.Current[AdWordsConst.DestUrlFieldName];

							//--------------
							// Campaign
							//--------------
							var campaign = new Campaign
								{
									Name = adsReader.Current[AdWordsConst.CampaignFieldName],
									OriginalID = adsReader.Current[AdWordsConst.CampaignIdFieldName]
								};
							ad.Fields.Add(GetExtraField("Campaign"), campaign);

							//--------------
							// Ad group
							//--------------
							var adGroup = new StringValue
								{
									Value = adsReader.Current[AdWordsConst.AdGroupFieldName],
									OriginalID = adsReader.Current[AdWordsConst.AdGroupIdFieldName],
								};
							adGroup.Fields.Add(GetExtraField("Campaign"), campaign);
							ad.Fields.Add(GetExtraField("AdGroup"), adGroup);

							//--------------
							// Creatives
							//--------------
							// composite creative and creative definition
							var compCreativeDefinition = new CompositeCreativeDefinition();
							var compCreative = new CompositeCreative();
							ad.CreativeDefinition = compCreativeDefinition;
							ad.CreativeDefinition.Creative = compCreative;

							// Display Url as text creative
							SingleCreative creative = new TextCreative
								{
									Text = adsReader.Current[AdWordsConst.DisplayURLFieldName],
									TextType = TextCreativeType.Url
								};
							compCreative.Parts.Add(GetCompositePartField("DisplayUrl"), creative);
							compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("DisplayUrl"), new TextCreativeDefinition { Creative = creative });

							if (String.Equals(Convert.ToString(adsReader.Current[AdWordsConst.AdTypeFieldName]), "Image ad"))
							{
								// Image as Image creative
								// format for example: Ad name: 468_60_Test7options_Romanian.swf; 468 x 60
								var imageParams = adsReader.Current[AdWordsConst.AdFieldName].Trim().Split(new[] { ':', ';' });

								creative = new ImageCreative { Image = imageParams[1].Trim() };
								compCreative.Parts.Add(GetCompositePartField("Image"), creative);
								compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("Image"), new ImageCreativeDefinition { Creative = creative, ImageSize = imageParams[2].Trim() });
							}
							else
							{
								// Title as Text creative
								creative = new TextCreative { Text = adsReader.Current.Ad, TextType = TextCreativeType.Text };
								compCreative.Parts.Add(GetCompositePartField("Title"), creative);
								compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("Title"), new TextCreativeDefinition { Creative = creative });

								// Description 1 as Text creative
								creative = new TextCreative { Text = adsReader.Current["Description line 1"], TextType = TextCreativeType.Text };
								compCreative.Parts.Add(GetCompositePartField("Description1"), creative);
								compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("Description1"), new TextCreativeDefinition { Creative = creative });

								// Description 2 as Text creative
								creative = new TextCreative { Text = adsReader.Current["Description line 2"], TextType = TextCreativeType.Text };
								compCreative.Parts.Add(GetCompositePartField("Description2"), creative);
								compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("Description2"), new TextCreativeDefinition { Creative = creative });
							}
							importedAds.Add(adId, ad);
						}
						metricsUnit.Ad = ad;

						// SERACH KEYWORD IN KEYWORD/ Placements  Dictionary
						var key = new KeywordPrimaryKey
							{
								AdgroupId = Convert.ToInt64(adsReader.Current[AdWordsConst.AdGroupIdFieldName]),
								KeywordId = Convert.ToInt64(adsReader.Current[AdWordsConst.KeywordIdFieldName]),
								CampaignId = Convert.ToInt64(adsReader.Current[AdWordsConst.CampaignIdFieldName])
							};

						if (key.KeywordId != Convert.ToInt64(Delivery.Parameters["KeywordContentId"]) && keywordsCache.ContainsKey(key.ToString()))
						{
							// Check if keyword exists in keywords cache (keywords report), if not - create new by ID
							var kwd = keywordsCache.ContainsKey(key.ToString()) ? keywordsCache[key.ToString()] :
									  new KeywordTarget { Value = adsReader.Current[AdWordsConst.KeywordIdFieldName] };

							// add keyword as a target to metrics
							metricsUnit.TargetDimensions.Add(GetTargetField("Keyword"), new TargetMatch { Target = kwd });
						}
						else
						{
							var placement = placementsCache.ContainsKey(key.ToString()) ? placementsCache[key.ToString()] :
									  new PlacementTarget
										  {
											  Value = adsReader.Current[AdWordsConst.KeywordIdFieldName],
											  PlacementType = PlacementType.Automatic
										  };

							// add placement as a target to metrics
							metricsUnit.TargetDimensions.Add(GetTargetField("Keyword"), new TargetMatch { Target = placement });
						}

						// metrics measures
						metricsUnit.MeasureValues = new Dictionary<Measure, double>();
						metricsUnit.MeasureValues.Add(GetMeasure("Clicks"), Convert.ToInt64(adsReader.Current.Clicks));
						metricsUnit.MeasureValues.Add(GetMeasure("Cost"), Convert.ToInt64(adsReader.Current.Cost) / 1000000);
						metricsUnit.MeasureValues.Add(GetMeasure("Impressions"), Convert.ToDouble(adsReader.Current[AdWordsConst.AvgPositionFieldName]));
						metricsUnit.MeasureValues.Add(GetMeasure("AveragePosition"), Convert.ToInt64(adsReader.Current.Clicks));
						metricsUnit.MeasureValues.Add(GetMeasure(_googleMeasuresDic[AdWordsConst.ConversionOnePerClickFieldName]), Convert.ToDouble(adsReader.Current[AdWordsConst.ConversionOnePerClickFieldName]));
						metricsUnit.MeasureValues.Add(GetMeasure(_googleMeasuresDic[AdWordsConst.ConversionManyPerClickFieldName]), Convert.ToDouble(adsReader.Current[AdWordsConst.ConversionManyPerClickFieldName]));

						// Inserting conversion values
						string conversionKey = String.Format("{0}#{1}", ad.OriginalID, adsReader.Current[AdWordsConst.KeywordIdFieldName]);
						if (importedAdsWithConv.ContainsKey(conversionKey))
						{
							var conversionDic = importedAdsWithConv[conversionKey];
							foreach (var pair in conversionDic.Where(pair => _googleMeasuresDic.ContainsKey(pair.Key)))
							{
								metricsUnit.MeasureValues.Add(GetMeasure(_googleMeasuresDic[pair.Key]), pair.Value);
							}
						}

						// currency
						metricsUnit.Currency = new Currency { Code = Convert.ToString(adsReader.Current.Currency) };

						// import metrics
						ImportManager.ImportMetrics(metricsUnit);
					}

					currentOutput.Checksum = totals;
					ImportManager.EndImport();
				}
				#endregion
			}
			return ServiceOutcome.Success;
		} 
		#endregion

		#region Private Methods
		private Dictionary<string, KeywordTarget> LoadKeywords(DeliveryFile file, string[] headers)
		{
			if (file == null)
				throw new ArgumentException("Keywords delivery file does not exist");

			var keywordsCache = new Dictionary<string, KeywordTarget>();
			using (var keywordsReader = new CsvDynamicReader(file.OpenContents(compression: FileCompression.Gzip), headers))
			{
				keywordsReader.MatchExactColumns = false;
				while (keywordsReader.Read())
				{
					if (keywordsReader.Current[AdWordsConst.KeywordIdFieldName] == AdWordsConst.EOF)
						break;
					var keywordPrimaryKey = new KeywordPrimaryKey
					{
						KeywordId = Convert.ToInt64(keywordsReader.Current[AdWordsConst.KeywordIdFieldName]),
						AdgroupId = Convert.ToInt64(keywordsReader.Current[AdWordsConst.AdGroupIdFieldName]),
						CampaignId = Convert.ToInt64(keywordsReader.Current[AdWordsConst.CampaignIdFieldName])
					};
					var keyword = new KeywordTarget
					{
						//OriginalID = keywordsReader.Current[AdWordsConst.KeywordIdFieldName],
						Value = keywordsReader.Current[AdWordsConst.KeywordFieldName],
						MatchType = Enum.Parse(typeof(KeywordMatchType), keywordsReader.Current[AdWordsConst.MatchTypeFieldName]),
						Fields = new Dictionary<EdgeField, object>()
						//Status = kwd_Status_Data[keywordPrimaryKey.ToString()]

					};
					keyword.Fields.Add(GetExtraField("OriginalID"), keywordsReader.Current[AdWordsConst.KeywordIdFieldName]);
					keyword.Fields.Add(GetExtraField("QualityScore"), keywordsReader.Current[AdWordsConst.QualityScoreFieldName]);
					keyword.Fields.Add(GetExtraField("DestinationUrl"), keywordsReader.Current[AdWordsConst.DestUrlFieldName]);

					//keyword.QualityScore = Convert.ToString(keywordsReader.Current[AdWordsConst.QualityScoreFieldName]);
					//string matchType = keywordsReader.Current[AdWordsConst.MatchTypeFieldName];

					//Setting Tracker for Keyword
					//if (!String.IsNullOrWhiteSpace(Convert.ToString(keywordsReader.Current[AdWordsConst.DestUrlFieldName])))
					//{
					//	keyword.DestinationUrl = Convert.ToString(keywordsReader.Current[AdWordsConst.DestUrlFieldName]);
					//}
					keywordsCache.Add(keywordPrimaryKey.ToString(), keyword);
				}
			}
			return keywordsCache;
		}

		private Dictionary<string, PlacementTarget> LoadPlacements(DeliveryFile file, string[] headers)
		{
			if (file == null)
				throw new ArgumentException("Placement delivery file does not exist");

			var placementsCache = new Dictionary<string, PlacementTarget>();
			using (var placementsReader = new CsvDynamicReader(file.OpenContents(compression: FileCompression.Gzip), headers))
			{
				while (placementsReader.Read())
				{
					if (placementsReader.Current[AdWordsConst.KeywordIdFieldName] == AdWordsConst.EOF)
						break;
					var placementPrimaryKey = new KeywordPrimaryKey
					{
						KeywordId = Convert.ToInt64(placementsReader.Current[AdWordsConst.KeywordIdFieldName]),
						AdgroupId = Convert.ToInt64(placementsReader.Current[AdWordsConst.AdGroupIdFieldName]),
						CampaignId = Convert.ToInt64(placementsReader.Current[AdWordsConst.CampaignIdFieldName])
					};
					var placement = new PlacementTarget
					{
						//OriginalID = placementsReader.Current[AdWordsConst.KeywordIdFieldName],
						Value = placementsReader.Current[AdWordsConst.PlacementFieldName],
						PlacementType = PlacementType.Managed,
						Fields = new Dictionary<EdgeField, object>()
						// Status = placement_kwd_Status_Data[placementPrimaryKey.ToString()]
					};
					placement.Fields.Add(GetExtraField("OriginalID"), placementsReader.Current[AdWordsConst.KeywordIdFieldName]);
					placement.Fields.Add(GetExtraField("DestinationUrl"), placementsReader.Current[AdWordsConst.DestUrlFieldName]);
					//Setting Tracker for placment
					//if (!String.IsNullOrWhiteSpace(Convert.ToString(placementsReader.Current[AdWordsConst.DestUrlFieldName])))
					//	placement.DestinationUrl = Convert.ToString(placementsReader.Current[AdWordsConst.DestUrlFieldName]);

					placementsCache.Add(placementPrimaryKey.ToString(), placement);
				}
			}
			return placementsCache;
		}

		private Dictionary<string, Dictionary<string, long>> LoadConversions(DeliveryFile file, string[] headers)
		{
			if (file == null)
				throw new ArgumentException("Ad conversions delivery file does not exist");

			var importedAdsWithConv = new Dictionary<string, Dictionary<string, long>>();
			using (var conversionsReader = new CsvDynamicReader(file.OpenContents(compression: FileCompression.Gzip), headers))
			{
				while (conversionsReader.Read())
				{
					if (conversionsReader.Current[AdWordsConst.AdIDFieldName] == AdWordsConst.EOF) break; // if end of report

					string conversionKey = String.Format("{0}#{1}", conversionsReader.Current[AdWordsConst.AdIDFieldName], conversionsReader.Current[AdWordsConst.KeywordIdFieldName]);
					var trackingPurpose = Convert.ToString(conversionsReader.Current[AdWordsConst.ConversionTrackingPurposeFieldName]);
					var manyPerClick = Convert.ToInt64(conversionsReader.Current[AdWordsConst.ConversionManyPerClickFieldName]);

					if (!importedAdsWithConv.ContainsKey(conversionKey))
					{
						// add new conversion key with new dictionary of tracking purpose and clicks
						importedAdsWithConv.Add(conversionKey, new Dictionary<string, long> { { trackingPurpose, manyPerClick } });
					}
					else // if conversion key exists
					{
						if (importedAdsWithConv[conversionKey].ContainsKey(trackingPurpose))
						{
							// if purpose exists --> sum to existing value
							importedAdsWithConv[conversionKey][trackingPurpose] += manyPerClick;
						}
						else
						{
							// create new entry for new tracking purpose
							importedAdsWithConv[conversionKey].Add(trackingPurpose, manyPerClick);
						}
					}
				}
			}
			return importedAdsWithConv;
		} 
		#endregion
    }
}


