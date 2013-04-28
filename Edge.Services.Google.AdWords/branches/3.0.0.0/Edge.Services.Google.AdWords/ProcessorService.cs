﻿using System;
using System.Collections.Generic;
using System.Configuration;
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

		private readonly Dictionary<string, Dictionary<string, long>> _importedAdsWithConv = new Dictionary<string, Dictionary<string, long>>();
		private readonly Dictionary<string, KeywordTarget> _keywordsCache = new Dictionary<string, KeywordTarget>();
		private readonly Dictionary<string, PlacementTarget> _placementsCache = new Dictionary<string, PlacementTarget>();
		private readonly Dictionary<string, Ad> _importedAds = new Dictionary<string, Ad>();
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

			//bool includeConversionTypes = Boolean.Parse(Delivery.Parameters["includeConversionTypes"].ToString());
			//bool includeDisplaytData = Boolean.Parse(Delivery.Parameters["includeDisplaytData"].ToString());

			//Status Members
			//var kwd_Status_Data = new Dictionary<string, ObjectStatus>();
			//var placement_kwd_Status_Data = new Dictionary<string, ObjectStatus>();
			//var adGroup_Status_Data = new Dictionary<Int64, ObjectStatus>();
			//var ad_Status_Data = new Dictionary<Int64, ObjectStatus>();
			//var campaign_Status_Data = new Dictionary<Int64, ObjectStatus>();

			using (ImportManager = new MetricsDeliveryManager(InstanceID, EdgeTypes, new MetricsDeliveryManagerOptions()))
			//{

			//	MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
			//	MeasureOptionsOperator = OptionsOperator.Not,
			//	SegmentOptions = Data.Objects.SegmentOptions.All,
			//	SegmentOptionsOperator = OptionsOperator.And
			//}))
			{
				ImportManager.BeginImport(Delivery, GetSampleMetrics());
				
				var requiredHeaders = new[] { AdWordsConst.AdPreRequiredHeader };
				var totals = new Dictionary<string, double>();

				// Getting Keywords Data
				LoadKeywords(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT]], requiredHeaders);

				// Getting Placements Data
				LoadPlacements(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT]], requiredHeaders);

				// Getting Conversions Data ( for ex. signup , purchase )
				LoadConversions(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv"], requiredHeaders);

				#region Getting Ads Data

				var adPerformanceFile = Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT]];
				var adsReader = new CsvDynamicReader(adPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
				var currentOutput = Delivery.Outputs.First();
				Mappings.OnFieldRequired = field => adsReader.Current[field];
				_importedAds.Clear();

				using (adsReader)
				{
					while (adsReader.Read() && adsReader.Current[AdWordsConst.AdIDFieldName] != AdWordsConst.EOF)
					{
						var metricUnit = CreateMetricsUnit(adsReader);
						metricUnit.Output = currentOutput;

						ImportManager.ImportMetrics(metricUnit);
					}

					currentOutput.Checksum = totals;
					ImportManager.EndImport();
				}
				#endregion
			}
			return ServiceOutcome.Success;
		}

		protected override MetricsUnit GetSampleMetrics()
		{
			var headers = new[] { AdWordsConst.AdPreRequiredHeader };
			
			// load sample keywords
			var file = new DeliveryFile {Location = Configuration.Parameters.Get<string>("KeywordSampleFile")};
			LoadKeywords(file, headers, FileCompression.None);
			
			// load ad
			using (var adsReader = new CsvDynamicReader(Configuration.Parameters.Get<string>("AdSampleFile"), headers))
			{
				if (adsReader.Read())
				{
					return CreateMetricsUnit(adsReader);
				}
			}
			throw new ConfigurationErrorsException(String.Format("Failed to read sample metrics from file: {0}", Configuration.Parameters.Get<string>("AdSampleFile")));
		}
		#endregion

		#region Private Methods
		private void LoadKeywords(DeliveryFile file, string[] headers, FileCompression compression = FileCompression.Gzip)
		{
			if (file == null)
				throw new ArgumentException("Keywords delivery file does not exist");

			_keywordsCache.Clear();
			using (var keywordsReader = new CsvDynamicReader(file.OpenContents(compression: compression), headers))
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
						Fields = new Dictionary<EdgeField, object>(),
						EdgeType = GetEdgeType("KeywordTarget"),
						TK = keywordsReader.Current[AdWordsConst.KeywordIdFieldName]

					};
					keyword.Fields.Add(GetExtraField("OriginalID"), keywordsReader.Current[AdWordsConst.KeywordIdFieldName]);
					keyword.Fields.Add(GetExtraField("QualityScore"), keywordsReader.Current[AdWordsConst.QualityScoreFieldName]);
					keyword.Fields.Add(GetExtraField("DestinationUrl"), keywordsReader.Current[AdWordsConst.DestUrlFieldName]);

					_keywordsCache.Add(keywordPrimaryKey.ToString(), keyword);
				}
			}
		}

		private void LoadPlacements(DeliveryFile file, string[] headers)
		{
			if (file == null)
				throw new ArgumentException("Placement delivery file does not exist");

			_placementsCache.Clear();
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
						Fields = new Dictionary<EdgeField, object>(),
						EdgeType = GetEdgeType("PlacementTarget"),
						TK = placementsReader.Current[AdWordsConst.PlacementIdFieldName]
					};
					placement.Fields.Add(GetExtraField("OriginalID"), placementsReader.Current[AdWordsConst.KeywordIdFieldName]);
					placement.Fields.Add(GetExtraField("DestinationUrl"), placementsReader.Current[AdWordsConst.DestUrlFieldName]);
					
					_placementsCache.Add(placementPrimaryKey.ToString(), placement);
				}
			}
		}

		private void LoadConversions(DeliveryFile file, string[] headers)
		{
			// TODO - throw exception if file does not exists
			//if (file == null)
			//	throw new ArgumentException("Ad conversions delivery file does not exist");

			if (file == null) return;

			_importedAdsWithConv.Clear();
			using (var conversionsReader = new CsvDynamicReader(file.OpenContents(compression: FileCompression.Gzip), headers))
			{
				while (conversionsReader.Read())
				{
					if (conversionsReader.Current[AdWordsConst.AdIDFieldName] == AdWordsConst.EOF) break; // if end of report

					string conversionKey = String.Format("{0}#{1}", conversionsReader.Current[AdWordsConst.AdIDFieldName], conversionsReader.Current[AdWordsConst.KeywordIdFieldName]);
					var trackingPurpose = Convert.ToString(conversionsReader.Current[AdWordsConst.ConversionTrackingPurposeFieldName]);
					var manyPerClick = Convert.ToInt64(conversionsReader.Current[AdWordsConst.ConversionManyPerClickFieldName]);

					if (!_importedAdsWithConv.ContainsKey(conversionKey))
					{
						// add new conversion key with new dictionary of tracking purpose and clicks
						_importedAdsWithConv.Add(conversionKey, new Dictionary<string, long> { { trackingPurpose, manyPerClick } });
					}
					else // if conversion key exists
					{
						if (_importedAdsWithConv[conversionKey].ContainsKey(trackingPurpose))
						{
							// if purpose exists --> sum to existing value
							_importedAdsWithConv[conversionKey][trackingPurpose] += manyPerClick;
						}
						else
						{
							// create new entry for new tracking purpose
							_importedAdsWithConv[conversionKey].Add(trackingPurpose, manyPerClick);
						}
					}
				}
			}
		}

		private Ad CreateAd(CsvDynamicReader adsReader)
		{
			//--------------
			// Ad
			//--------------
			var ad = new Ad
			{
				OriginalID = adsReader.Current[AdWordsConst.AdIDFieldName],
				Channel = GetChannel("Google"),
				Account = GetAccount("Bbinary"),
				Fields = new Dictionary<EdgeField, object>(),
				TK = adsReader.Current[AdWordsConst.AdIDFieldName], 
				EdgeType = GetEdgeType("Ad")
			};

			//--------------
			// Ad Type
			//--------------
			var adTypeColumnValue = adsReader.Current[AdWordsConst.AdTypeFieldName].ToString();
			var devicePreferenceColumnValue = adsReader.Current[AdWordsConst.AdDevicePreferenceFieldName].ToString();
			if (!_googleAdTypeDic.ContainsKey(adTypeColumnValue))
				throw new ArgumentException(String.Format("Unknown Ad type={0}", adTypeColumnValue));
			var adTypeEdgeValue = _googleAdTypeDic[adTypeColumnValue].ToString();

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
				OriginalID = adsReader.Current[AdWordsConst.CampaignIdFieldName],
				TK = adsReader.Current[AdWordsConst.CampaignIdFieldName],
				EdgeType = GetEdgeType("Campaign")
			};
			ad.Fields.Add(GetExtraField("Campaign"), campaign);

			//--------------
			// Ad group
			//--------------
			var adGroup = new StringValue
			{
				Value = adsReader.Current[AdWordsConst.AdGroupFieldName],
				OriginalID = adsReader.Current[AdWordsConst.AdGroupIdFieldName],
				Fields = new Dictionary<EdgeField, object>(),
				TK = adsReader.Current[AdWordsConst.AdGroupIdFieldName],
				EdgeType = GetEdgeType("AdGroup")
			};
			adGroup.Fields.Add(GetExtraField("Campaign"), campaign);
			ad.Fields.Add(GetExtraField("AdGroup"), adGroup);

			//---------------------
			// Composite Creatives
			//---------------------
			// composite creative and composite creative definition
			var compCreative = new CompositeCreative
			{
				Parts = new Dictionary<CompositePartField, SingleCreative>(),
				EdgeType = GetEdgeType("CompositeCreative")
			}; 
			var compCreativeDefinition = new CompositeCreativeDefinition
				{
					CreativeDefinitions = new Dictionary<CompositePartField, SingleCreativeDefinition>(),
					Creative = compCreative,
					EdgeType = GetEdgeType("CompositeCreativeDefinition")
				};
			
			//----------------------------------
			// Display Url as text creative
			//----------------------------------
			SingleCreative creative = new TextCreative
			{
				Text = adsReader.Current[AdWordsConst.DisplayURLFieldName],
				TextType = TextCreativeType.Url,
				EdgeType =  GetEdgeType("TextCreative"),
				TK = String.Format("{0}_{1}", TextCreativeType.Url, adsReader.Current[AdWordsConst.DisplayURLFieldName])
			};
			compCreative.Parts.Add(GetCompositePartField("DisplayUrlCreative"), creative);

			SingleCreativeDefinition definition = new TextCreativeDefinition
				{
					Creative = creative, 
					EdgeType = GetEdgeType("TextCreativeDefinition"),
					TK = adsReader.Current[AdWordsConst.DisplayURLFieldName]
				};
			compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("DisplayUrlDefinition"), definition);

			if (String.Equals(Convert.ToString(adsReader.Current[AdWordsConst.AdTypeFieldName]), "Image ad"))
			{
				//----------------------------------
				// Image as Image creative
				//----------------------------------
				// format for example: Ad name: 468_60_Test7options_Romanian.swf; 468 x 60
				var imageParams = adsReader.Current[AdWordsConst.AdFieldName].Trim().Split(new[] { ':', ';' });

				creative = new ImageCreative
					{
						Image = imageParams[1].Trim(),
						EdgeType = GetEdgeType("ImageCreative"),
						TK = imageParams[1].Trim()
					};
				compCreative.Parts.Add(GetCompositePartField("SingleCreative"), creative);

				definition = new ImageCreativeDefinition
					{
						Creative = creative, 
						ImageSize = imageParams[2].Trim(), 
						EdgeType = GetEdgeType("ImageCreativeDefinition"),
						TK = String.Format("{0}_{1}", imageParams[1].Trim(), imageParams[2].Trim())
					};
				compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("SingleCreativeDefinition"), definition );
			}
			else
			{
				//----------------------------------
				// Title as Text creative
				//----------------------------------
				creative = new TextCreative
					{
						Text = adsReader.Current.Ad, 
						TextType = TextCreativeType.Text,
						EdgeType = GetEdgeType("TextCreative"),
						TK = String.Format("{0}_{1}", TextCreativeType.Text, adsReader.Current.Ad)
					};
				compCreative.Parts.Add(GetCompositePartField("SingleCreative"), creative);

				definition = new TextCreativeDefinition
					{
						Creative = creative, 
						EdgeType = GetEdgeType("TextCreativeDefinition"),
						TK = adsReader.Current.Ad
					};
				compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("SingleCreativeDefinition"), definition);

				//----------------------------------
				// Description 1 as Text creative
				//----------------------------------
				creative = new TextCreative
					{
						Text = adsReader.Current["Description line 1"], 
						TextType = TextCreativeType.Text,
						EdgeType = GetEdgeType("TextCreative"),
						TK = String.Format("{0}_{1}", TextCreativeType.Text, adsReader.Current["Description line 1"])
					};
				compCreative.Parts.Add(GetCompositePartField("Desc1Creative"), creative);

				definition = new TextCreativeDefinition
					{
						Creative = creative, 
						EdgeType = GetEdgeType("TextCreativeDefinition"),
						TK = adsReader.Current["Description line 1"]
					};
				compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("Desc1Definition"),definition);

				//----------------------------------
				// Description 2 as Text creative
				//----------------------------------
				creative = new TextCreative
					{
						Text = adsReader.Current["Description line 2"], 
						TextType = TextCreativeType.Text,
						EdgeType = GetEdgeType("TextCreative"),
						TK = String.Format("{0}_{1}", TextCreativeType.Text, adsReader.Current["Description line 2"])
					};
				compCreative.Parts.Add(GetCompositePartField("Desc2Creative"), creative);

				definition = new TextCreativeDefinition
					{
						Creative = creative, 
						EdgeType = GetEdgeType("TextCreativeDefinition"),
						TK = adsReader.Current["Description line 2"]
					};
				compCreativeDefinition.CreativeDefinitions.Add(GetCompositePartField("Desc2Definition"), definition);
			}

			SetCompositeCreativeTk(compCreative);
			SetCompositeCreativeDefinitionTk(compCreativeDefinition);
			ad.CreativeDefinition = compCreativeDefinition;
			ad.CreativeDefinition.Creative = compCreative;

			// add Ad to cache of Ads
			_importedAds.Add(ad.OriginalID, ad);
			return ad;
		}

		private MetricsUnit CreateMetricsUnit(CsvDynamicReader adsReader)
		{
			// get already existing or create new Ad
			string adId = adsReader.Current[AdWordsConst.AdIDFieldName];
			Ad ad = _importedAds.ContainsKey(adId) ? _importedAds[adId] : CreateAd(adsReader);

			// create metrics unit
			var metricsUnit = new MetricsUnit
			{
				GetEdgeField = GetEdgeField,
				Ad = ad,
				Channel = GetChannel("Google"),		// TODO change from Delivery
				Account = GetAccount("Bbinary"),	// TODO change from Delivery
				TimePeriodStart = Delivery.TimePeriodStart,
				TimePeriodEnd = Delivery.TimePeriodEnd,
				Currency = new Currency { Code = Convert.ToString(adsReader.Current.Currency) }
			};

			// add keyword or placement as a target to metrics
			var target = GetTarget(adsReader);
			metricsUnit.Dimensions.Add(GetTargetField("TargetMatch"), new TargetMatch
			{
				Target = target,
				EdgeType = GetEdgeType("TargetMatch"),
				TK = target.TK
			});

			// metrics measures
			metricsUnit.MeasureValues = new Dictionary<Measure, double>();
			metricsUnit.MeasureValues.Add(GetMeasure("Clicks"), Convert.ToInt64(adsReader.Current.Clicks));
			metricsUnit.MeasureValues.Add(GetMeasure("Cost"), Convert.ToInt64(adsReader.Current.Cost) / 1000000);
			metricsUnit.MeasureValues.Add(GetMeasure("Impressions"), Convert.ToDouble(adsReader.Current[AdWordsConst.AvgPositionFieldName]));
			metricsUnit.MeasureValues.Add(GetMeasure("AveragePosition"), Convert.ToInt64(adsReader.Current.Clicks));
			metricsUnit.MeasureValues.Add(GetMeasure(_googleMeasuresDic[AdWordsConst.ConversionOnePerClickFieldName]), Convert.ToDouble(adsReader.Current[AdWordsConst.ConversionOnePerClickFieldName]));
			metricsUnit.MeasureValues.Add(GetMeasure(_googleMeasuresDic[AdWordsConst.ConversionManyPerClickFieldName]), Convert.ToDouble(adsReader.Current[AdWordsConst.ConversionManyPerClickFieldName]));

			// add conversion values
			string conversionKey = String.Format("{0}#{1}", ad.OriginalID, adsReader.Current[AdWordsConst.KeywordIdFieldName]);
			if (_importedAdsWithConv.ContainsKey(conversionKey))
			{
				var conversionDic = _importedAdsWithConv[conversionKey];
				foreach (var pair in conversionDic.Where(pair => _googleMeasuresDic.ContainsKey(pair.Key)))
				{
					metricsUnit.MeasureValues.Add(GetMeasure(_googleMeasuresDic[pair.Key]), pair.Value);
				}
			}

			return metricsUnit;
		}

		private Target GetTarget(CsvDynamicReader adsReader)
		{
			Target target;

			// get keyword by key from keyword or placement dictionary
			var key = new KeywordPrimaryKey
			{
				AdgroupId = Convert.ToInt64(adsReader.Current[AdWordsConst.AdGroupIdFieldName]),
				KeywordId = Convert.ToInt64(adsReader.Current[AdWordsConst.KeywordIdFieldName]),
				CampaignId = Convert.ToInt64(adsReader.Current[AdWordsConst.CampaignIdFieldName])
			};

			if (key.KeywordId != Convert.ToInt64(Delivery.Parameters["KeywordContentId"]) && _keywordsCache.ContainsKey(key.ToString()))
			{
				// Check if keyword exists in keywords cache (keywords report), if not - create new by ID
				target = _keywordsCache.ContainsKey(key.ToString()) ? _keywordsCache[key.ToString()] :
							new KeywordTarget
							{
								Value = adsReader.Current[AdWordsConst.KeywordIdFieldName],
								MatchType = KeywordMatchType.Unidentified,
								EdgeType = GetEdgeType("KeywordTarget"),
								TK = String.Format("{0}_{1}", KeywordMatchType.Unidentified, adsReader.Current[AdWordsConst.KeywordIdFieldName]),
								Fields = new Dictionary<EdgeField, object>()
							};
					
			}
			else
			{
				// Check if placement exists in placement cache (placements report), if not - create new by ID
				target = _placementsCache.ContainsKey(key.ToString()) ? _placementsCache[key.ToString()] :
							new PlacementTarget
							{
								Value = adsReader.Current[AdWordsConst.KeywordIdFieldName],
								PlacementType = PlacementType.Automatic,
								EdgeType = GetEdgeType("PlacementTarget"),
								TK = String.Format("{0}_{1}", PlacementType.Automatic, adsReader.Current[AdWordsConst.KeywordIdFieldName]),
								Fields = new Dictionary<EdgeField, object>()
							};
			}
			if (!target.Fields.ContainsKey(GetExtraField("OriginalID"))) 
				target.Fields.Add(GetExtraField("OriginalID"), adsReader.Current[AdWordsConst.KeywordIdFieldName]);

			if (!target.Fields.ContainsKey(GetExtraField("DestinationUrl"))) 
				target.Fields.Add(GetExtraField("DestinationUrl"), adsReader.Current[AdWordsConst.DestUrlFieldName]);
			return target;
		}

		private static void SetCompositeCreativeDefinitionTk(CompositeCreativeDefinition compCreativeDefinition)
		{
			foreach (var part in compCreativeDefinition.CreativeDefinitions)
			{
				compCreativeDefinition.TK = String.Format("{0}_{1}", compCreativeDefinition.TK, part.Value.TK);
			}
			if (compCreativeDefinition.TK.Length > 1) compCreativeDefinition.TK = compCreativeDefinition.TK.Remove(0, 1);
		}

		private static void SetCompositeCreativeTk(CompositeCreative compCreative)
		{
			foreach (var part in compCreative.Parts)
			{
				compCreative.TK = String.Format("{0}_{1}", compCreative.TK, part.Value.TK);
			}
			if (compCreative.TK.Length > 1) compCreative.TK = compCreative.TK.Remove(0, 1);
		}
		#endregion
    }
}


