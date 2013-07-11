using System;
using System.Collections.Generic;
using System.Configuration;
using Edge.Core.Services;
using Edge.Core.Utilities;
using Edge.Data.Pipeline.Mapping;
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
	public class ProcessorService : AutoMetricsProcessorService
    {
		#region Data Members
		static Dictionary<string, string> _googleMeasuresDic;

		private readonly Dictionary<string, Dictionary<string, long>> _importedAdsWithConv = new Dictionary<string, Dictionary<string, long>>();
		private readonly Dictionary<string, KeywordTarget> _keywordsCache = new Dictionary<string, KeywordTarget>();
		private readonly Dictionary<string, PlacementTarget> _placementsCache = new Dictionary<string, PlacementTarget>();
		private readonly Dictionary<string, Ad> _importedAds = new Dictionary<string, Ad>();

		protected MappingContainer KeywordMappings;
		protected MappingContainer PlacementMappings;
		
		private CsvDynamicReader _adsReader;
		#endregion

		#region Ctor
		public ProcessorService()
		{
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
		} 
		#endregion

		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			Log("Starting Google.AdWords.ProcessorService", LogMessageType.Debug);
			InitMappings();

			Mappings.OnMappingApplied = SetEdgeType;

			if (!Mappings.Objects.TryGetValue(typeof(KeywordTarget), out KeywordMappings))
				throw new MappingConfigurationException("Missing mapping definition for KeywordTarget.", "Object");

			if (!Mappings.Objects.TryGetValue(typeof(PlacementTarget), out PlacementMappings))
				throw new MappingConfigurationException("Missing mapping definition for PlacementTarget.", "Object");

			if (!Mappings.Objects.TryGetValue(typeof(MetricsUnit), out MetricsMappings))
				throw new MappingConfigurationException("Missing mapping definition for MetricsUnit.", "Object");

			if (!Mappings.Objects.TryGetValue(typeof(Signature), out SignatureMappings))
				throw new MappingConfigurationException("Missing mapping definition for Signature.", "Object");

			using (ImportManager = new MetricsDeliveryManager(InstanceID, EdgeTypes, new MetricsDeliveryManagerOptions()))
			//{

			//	MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
			//	MeasureOptionsOperator = OptionsOperator.Not,
			//	SegmentOptions = Data.Objects.SegmentOptions.All,
			//	SegmentOptionsOperator = OptionsOperator.And
			//}))
			{
				ImportManager.BeginImport(Delivery, GetSampleMetrics());
				Log("Objects and Metrics tables are created", LogMessageType.Debug);
				Progress = 0.1;
				
				var requiredHeaders = new[] { AdWordsConst.AdPreRequiredHeader };

				// Getting Keywords Data
				Log("Start loading keywords", LogMessageType.Debug);
				LoadKeywords(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT]], requiredHeaders);
				Log("Finished loading keywords", LogMessageType.Debug);
				Progress = 0.3;
				
				// Getting Placements Data
				Log("Start loading placements", LogMessageType.Debug);
				LoadPlacements(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT]], requiredHeaders);
				Log("Finished loading placements", LogMessageType.Debug);

				// Getting Conversions Data ( for ex. signup , purchase )
				Log("Start loading conversions", LogMessageType.Debug);
				LoadConversions(Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv"], requiredHeaders);
				Log("Finished loading conversions", LogMessageType.Debug);
				Progress = 0.4;

				#region Getting Ads Data and Import Metrics
				Log("Start loading Ads", LogMessageType.Debug);
				
				var adPerformanceFile = Delivery.Files[GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT]];
				_adsReader = new CsvDynamicReader(adPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
				Mappings.OnFieldRequired = field => _adsReader.Current[field];
				_importedAds.Clear();

				using (_adsReader)
				{
					Mappings.OnFieldRequired = fieldName => _adsReader.Current[fieldName];
					while (_adsReader.Read() && _adsReader.Current[AdWordsConst.AdIDFieldName] != AdWordsConst.EOF)
					{
						ProcessMetrics();

						// add Ad if it is not exists yet
						if (!_importedAds.ContainsKey(CurrentMetricsUnit.Ad.OriginalID))
							_importedAds.Add(CurrentMetricsUnit.Ad.OriginalID, CurrentMetricsUnit.Ad);
					}
					//Progress = 0.8;
					//Log("Start importing objects", LogMessageType.Debug);
					ImportManager.EndImport();
					//Log("Finished importing objects", LogMessageType.Debug);
				}
				#endregion
			}
			return ServiceOutcome.Success;
		}

		protected override void AddExternalMethods()
		{
			base.AddExternalMethods();
			Mappings.ExternalMethods.Add("GetAd", new Func<dynamic, Ad>(GetAd));
			Mappings.ExternalMethods.Add("GetAdType", new Func<dynamic, dynamic, string>(GetAdType));
			Mappings.ExternalMethods.Add("GetKeywordTarget", new Func<Target>(GetKeywordTarget));
			Mappings.ExternalMethods.Add("GetPlacementTarget", new Func<Target>(GetPlacementTarget));
			Mappings.ExternalMethods.Add("GetImageData", new Func<dynamic, dynamic, string>(GetImageData));
			Mappings.ExternalMethods.Add("GetConversion", new Func<dynamic, dynamic, dynamic, dynamic, double>(GetConversion));
		}

		protected override MetricsUnit GetSampleMetrics()
		{
			var headers = new[] { AdWordsConst.AdPreRequiredHeader };
			
			// load sample keywords
			var file = new DeliveryFile {Location = Configuration.Parameters.Get<string>("KeywordSampleFile")};
			LoadKeywords(file, headers, FileCompression.None);

			file = new DeliveryFile { Location = Configuration.Parameters.Get<string>("PlacementSampleFile") };
			LoadPlacements(file, new[] { AdWordsConst.AutoPlacRequiredHeader }, FileCompression.None);
			
			// load ad
			using (_adsReader = new CsvDynamicReader(Configuration.Parameters.Get<string>("AdSampleFile"), headers))
			{
				Mappings.OnFieldRequired = fieldName => _adsReader.Current[fieldName];
				if (_adsReader.Read())
				{
					CurrentMetricsUnit = new MetricsUnit { GetEdgeField = GetEdgeField, Output = new DeliveryOutput()};
					MetricsMappings.Apply(CurrentMetricsUnit);
					return CurrentMetricsUnit;
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
				Mappings.OnFieldRequired = fieldName => keywordsReader.Current[fieldName];
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

					var keyword = new KeywordTarget();
					KeywordMappings.Apply(keyword);

					_keywordsCache.Add(keywordPrimaryKey.ToString(), keyword);
				}
			}
		}

		private void LoadPlacements(DeliveryFile file, string[] headers, FileCompression compression = FileCompression.Gzip)
		{
			if (file == null)
				throw new ArgumentException("Placement delivery file does not exist");

			_placementsCache.Clear();
			using (var placementsReader = new CsvDynamicReader(file.OpenContents(compression: compression), headers))
			{
				Mappings.OnFieldRequired = fieldName => placementsReader.Current[fieldName];
				placementsReader.MatchExactColumns = false;
				while (placementsReader.Read())
				{
					if (placementsReader.Current[AdWordsConst.PlacementIdFieldName] == AdWordsConst.EOF)
						break;
					var placementPrimaryKey = new KeywordPrimaryKey
					{
						KeywordId = Convert.ToInt64(placementsReader.Current[AdWordsConst.PlacementIdFieldName]),
						AdgroupId = Convert.ToInt64(placementsReader.Current[AdWordsConst.AdGroupIdFieldName]),
						CampaignId = Convert.ToInt64(placementsReader.Current[AdWordsConst.CampaignIdFieldName])
					};
					
					var placement = new PlacementTarget();
					PlacementMappings.Apply(placement);

					_placementsCache.Add(placementPrimaryKey.ToString(), placement);
				}
			}
		}

		private void LoadConversions(DeliveryFile file, string[] headers)
		{
			if (file == null)
				throw new ArgumentException("Ad conversions delivery file does not exist");

			_importedAdsWithConv.Clear();
			using (var conversionsReader = new CsvDynamicReader(file.OpenContents(compression: FileCompression.Gzip), headers))
			{
				while (conversionsReader.Read())
				{
					if (conversionsReader.Current[AdWordsConst.AdIDFieldName] == AdWordsConst.EOF) break; // if end of report

					string conversionKey = String.Format("{0}#{1}#{2}", conversionsReader.Current[AdWordsConst.AdIDFieldName], 
																		conversionsReader.Current[AdWordsConst.KeywordIdFieldName],
																		conversionsReader.Current[AdWordsConst.DateFieldName]);
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
		
		#endregion

		#region Private: Create metrics unit with no mapping
		private MetricsUnit CreateMetricsUnitNoMapping(CsvDynamicReader adsReader)
		{
			// get already existing or create new Ad
			string adId = adsReader.Current[AdWordsConst.AdIDFieldName];
			Ad ad = _importedAds.ContainsKey(adId) ? _importedAds[adId] : CreateAd(adsReader);

			// create metrics unit
			var metricsUnit = new MetricsUnit
			{
				GetEdgeField = GetEdgeField,
				Ad = ad,
				Channel = GetChannel("Google"),		
				Account = GetAccount("Bbinary"),	
				TimePeriodStart = Delivery.TimePeriodStart,
				TimePeriodEnd = Delivery.TimePeriodEnd,
				Currency = new EdgeCurrency { Code = Convert.ToString(adsReader.Current.Currency) }
			};

			// add keyword or placement as a target to metrics
			var target = GetKeywordTarget();
			metricsUnit.Dimensions.Add(GetTargetField("TargetMatch"), new TargetMatch
			{
				Target = target,
				EdgeType = GetEdgeType("TargetMatch"),
				TK = target.TK
			});

			// metrics measures
			metricsUnit.MeasureValues = new Dictionary<Measure, double>();
			metricsUnit.MeasureValues.Add(GetMeasure("Clicks"), Convert.ToInt64(adsReader.Current.Clicks));
			metricsUnit.MeasureValues.Add(GetMeasure("Cost"), Convert.ToDouble(adsReader.Current.Cost) / 1000000);
			metricsUnit.MeasureValues.Add(GetMeasure("Impressions"), Convert.ToInt64(adsReader.Current.Impressions));
			metricsUnit.MeasureValues.Add(GetMeasure("AveragePosition"), Convert.ToDouble(adsReader.Current[AdWordsConst.AvgPositionFieldName]));
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
			var adTypeValue = adsReader.Current[AdWordsConst.AdTypeFieldName].ToString();
			var devicePreferenceColumnValue = adsReader.Current[AdWordsConst.AdDevicePreferenceFieldName].ToString();
			
			//is mobile ad ? 
			if (devicePreferenceColumnValue.Equals(AdWordsConst.AdDevicePreferenceMobileFieldValue))
				adTypeValue = string.Format("Mobile {0}", adTypeValue);

			ad.Fields.Add(GetExtraField("AdType"), new StringValue
				{
					Value = adTypeValue,
					TK = adTypeValue,
					EdgeType = GetEdgeType("AdType")
				});

			//------------------
			// Destination Url
			//------------------
			if (!String.IsNullOrWhiteSpace(adsReader.Current[AdWordsConst.DestUrlFieldName]))
				ad.MatchDestination = new Destination
			{
				Value = adsReader.Current[AdWordsConst.DestUrlFieldName],
				TK = adsReader.Current[AdWordsConst.DestUrlFieldName],
				EdgeType = GetEdgeType("Destination")
			};

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
			var compCreativeMatch = new CompositeCreativeMatch
			{
				CreativesMatches = new Dictionary<CompositePartField, SingleCreativeMatch>(),
				Creative = compCreative,
				EdgeType = GetEdgeType("CompositeCreativeMatch")
			};

			//----------------------------------
			// Display Url as text creative
			//----------------------------------
			SingleCreative creative = new TextCreative
			{
				Text = adsReader.Current[AdWordsConst.DisplayURLFieldName],
				//TextCreativeType = new TextCreativeType 
				//{ 
				//	Value = "Url", 
				//	TK =  "Url",
				//	EdgeType = GetEdgeType("TextCreativeType")
				//},
				EdgeType = GetEdgeType("TextCreative"),
				TK = String.Format("{0}_{1}", "Url", adsReader.Current[AdWordsConst.DisplayURLFieldName])
			};
			compCreative.Parts.Add(GetCompositePartField("DisplayUrlCreative"), creative);

			SingleCreativeMatch match = new TextCreativeMatch
			{
				Creative = creative,
				EdgeType = GetEdgeType("TextCreativeMatch"),
				TK = adsReader.Current[AdWordsConst.DisplayURLFieldName]
			};
			compCreativeMatch.CreativesMatches.Add(GetCompositePartField("DisplayUrlMatch"), match);

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

				match = new ImageCreativeMatch
				{
					Creative = creative,
					ImageSize = imageParams[2].Trim(),
					EdgeType = GetEdgeType("ImageCreativeDefinition"),
					TK = String.Format("{0}_{1}", imageParams[1].Trim(), imageParams[2].Trim())
				};
				compCreativeMatch.CreativesMatches.Add(GetCompositePartField("SingleCreativeMatch"), match);
			}
			else
			{
				//----------------------------------
				// Title as Text creative
				//----------------------------------
				creative = new TextCreative
				{
					Text = adsReader.Current.Ad,
					//TextCreativeType = new TextCreativeType
					//{
					//	Value = "Text",
					//	TK = "Text",
					//	EdgeType = GetEdgeType("TextCreativeType")
					//},
					EdgeType = GetEdgeType("TextCreative"),
					TK = String.Format("{0}_{1}", "Text", adsReader.Current.Ad)
				};
				compCreative.Parts.Add(GetCompositePartField("SingleCreative"), creative);

				match = new TextCreativeMatch
				{
					Creative = creative,
					EdgeType = GetEdgeType("TextCreativeMatch"),
					TK = adsReader.Current.Ad
				};
				compCreativeMatch.CreativesMatches.Add(GetCompositePartField("SingleCreativeMatch"), match);

				//----------------------------------
				// Description 1 as Text creative
				//----------------------------------
				creative = new TextCreative
				{
					Text = adsReader.Current["Description line 1"],
					//TextCreativeType = new TextCreativeType
					//{
					//	Value = "Text",
					//	TK = "Text",
					//	EdgeType = GetEdgeType("TextCreativeType")
					//},
					EdgeType = GetEdgeType("TextCreative"),
					TK = String.Format("{0}_{1}", "Text", adsReader.Current["Description line 1"])
				};
				compCreative.Parts.Add(GetCompositePartField("Desc1Creative"), creative);

				match = new TextCreativeMatch
				{
					Creative = creative,
					EdgeType = GetEdgeType("TextCreativeMatch"),
					TK = adsReader.Current["Description line 1"]
				};
				compCreativeMatch.CreativesMatches.Add(GetCompositePartField("Desc1Definition"), match);

				//----------------------------------
				// Description 2 as Text creative
				//----------------------------------
				creative = new TextCreative
				{
					Text = adsReader.Current["Description line 2"],
					//TextCreativeType = new TextCreativeType
					//{
					//	Value = "Text",
					//	TK = "Text",
					//	EdgeType = GetEdgeType("TextCreativeType")
					//},
					EdgeType = GetEdgeType("TextCreative"),
					TK = String.Format("{0}_{1}", "Text", adsReader.Current["Description line 2"])
				};
				compCreative.Parts.Add(GetCompositePartField("Desc2Creative"), creative);

				match = new TextCreativeMatch
				{
					Creative = creative,
					EdgeType = GetEdgeType("TextCreativeMatch"),
					TK = adsReader.Current["Description line 2"]
				};
				compCreativeMatch.CreativesMatches.Add(GetCompositePartField("Desc2Definition"), match);
			}

			SetCompositeCreativeTk(compCreative);
			SetCompositeCreativeDefinitionTk(compCreativeMatch);
			ad.CreativeMatch = compCreativeMatch;
			ad.CreativeMatch.Creative = compCreative;

			// add Ad to cache of Ads
			_importedAds.Add(ad.OriginalID, ad);
			return ad;
		}

		private static void SetCompositeCreativeDefinitionTk(CompositeCreativeMatch compCreativeMatch)
		{
			foreach (var part in compCreativeMatch.CreativesMatches)
			{
				compCreativeMatch.TK = String.Format("{0}_{1}", compCreativeMatch.TK, part.Value.TK);
			}
			if (compCreativeMatch.TK.Length > 1) compCreativeMatch.TK = compCreativeMatch.TK.Remove(0, 1);
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
		
		#region Scriptable Methods
		private Ad GetAd(dynamic adId)
		{
			if (_importedAds.ContainsKey(adId))
				return _importedAds[adId];

			return null;
		}

		private string GetAdType(dynamic adType, dynamic deviceRef)
		{
			// mobile device
			if (deviceRef.ToString() == AdWordsConst.AdDevicePreferenceMobileFieldValue)
				return String.Format("Mobile {0}", adType.ToString());

			return adType.ToString();
		}

		private Target GetKeywordTarget()
		{
			Target target;
			var id = _adsReader.Current[AdWordsConst.KeywordIdFieldName];

			// get keyword by key from keyword or placement dictionary
			var key = new KeywordPrimaryKey
			{
				AdgroupId = Convert.ToInt64(_adsReader.Current[AdWordsConst.AdGroupIdFieldName]),
				KeywordId = Convert.ToInt64(_adsReader.Current[AdWordsConst.KeywordIdFieldName]),
				CampaignId = Convert.ToInt64(_adsReader.Current[AdWordsConst.CampaignIdFieldName])
			};

			// TODO - what is it for???   key.KeywordId != Convert.ToInt64(Delivery.Parameters["KeywordContentId"])
			 
			// Check if keyword exists in Keywords cache, if YES - take it
			target = _keywordsCache.ContainsKey(key.ToString()) ? _keywordsCache[key.ToString()] :
					
					 // check if contains in Placement cache, if YES - null (because placement exists)
					 _placementsCache.ContainsKey(key.ToString()) ? null :
					 
					 // otherwise - create new Keyword by ID
					 new KeywordTarget
						{
							Value = id,
							MatchType = new KeywordMatchType
							{
								Value = "Unidentified",
								TK = "Unidentified",
								EdgeType = GetEdgeType("KeywordMatchType"),
							},
							EdgeType = GetEdgeType("KeywordTarget"),
							TK = id,
							Fields = new Dictionary<EdgeField, object> { {GetExtraField("OriginalID"), id} }
						};
			return target;
		}

		private Target GetPlacementTarget()
		{
			Target target;
			var id = _adsReader.Current[AdWordsConst.KeywordIdFieldName];

			// get keyword by key from keyword or placement dictionary
			var key = new KeywordPrimaryKey
			{
				AdgroupId = Convert.ToInt64(_adsReader.Current[AdWordsConst.AdGroupIdFieldName]),
				KeywordId = Convert.ToInt64(_adsReader.Current[AdWordsConst.KeywordIdFieldName]),
				CampaignId = Convert.ToInt64(_adsReader.Current[AdWordsConst.CampaignIdFieldName])
			};
			 
			// Check if placement exists in Placement cache, if YES - take it, otherwise - null
			target = _placementsCache.ContainsKey(key.ToString()) ? _placementsCache[key.ToString()] : null;
					
					 // otherwise - create new Placement by ID
					 //new PlacementTarget
					 //	   {
					 //		   Value = id,
					 //		   PlacementType = new PlacementType 
					 //		   {
					 //			   Value = "Automatic",
					 //			   TK = "Automatic",
					 //			   EdgeType = GetEdgeType("PlacementType"),
					 //		   },
					 //		   EdgeType = GetEdgeType("PlacementTarget"),
					 //		   TK = id,
					 //		   Fields = new Dictionary<EdgeField, object> { {GetExtraField("OriginalID"), id}}
					 //	   };
			return target;
		}

		private string GetImageData(dynamic ad, dynamic dataType)
		{
			var imageParams = ad.ToString().Trim().Split(new[] { ':', ';' });
			if (imageParams.Length < 3)
				throw  new Exception(String.Format("Invalid image ad format='{0}'", ad.ToString()));

			if (dataType.ToString() == "name")
				return imageParams[1];
			if (dataType.ToString() == "size")
				return imageParams[2];

			throw new Exception(String.Format("Unknown image data type request='{0}'", dataType.ToString()));
		}

		private double GetConversion(dynamic conversionName, dynamic adId, dynamic keywordId, dynamic dayCode)
		{
			var conversionKey = String.Format("{0}#{1}#{2}", adId, keywordId, dayCode);
			if (_importedAdsWithConv.ContainsKey(conversionKey))
			{
				Dictionary<string, long> conversionDic = _importedAdsWithConv[conversionKey];
				if (conversionDic.ContainsKey(conversionName.ToString()))
					return conversionDic[conversionName.ToString()];
			}
			return 0;
		}
		#endregion
    }
}


