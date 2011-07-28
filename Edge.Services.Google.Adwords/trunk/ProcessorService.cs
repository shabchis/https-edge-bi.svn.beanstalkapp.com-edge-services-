using System;
using System.Collections.Generic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using GA = Google.Api.Ads.AdWords.v201101;
using System.IO;
using Edge.Services.AdMetrics;

namespace Edge.Services.Google.AdWords
{
	class ProcessorService : PipelineService
	{
		//ErrorFile _keywordErrorFile = new ErrorFile("Errors_KeywordPrimaryKey", new List<string> { "AdgroupId", "KeywordId", "CampaignId" }, @"D:\");

		static ExtraField NetworkType = new ExtraField() { ColumnIndex = 1, Name = "NetworkType" };
		static ExtraField AdType = new ExtraField() { ColumnIndex = 2, Name = "adType" };
		static Dictionary<string, int> GoogleAdTypeDic;
		static Dictionary<string, string> GoogleMeasuresDic;

		public ProcessorService()
		{

			GoogleAdTypeDic = new Dictionary<string, int>()
			{
				{"Text ad",1},
				{"Flash",2},
				{"Image ad",3},
				{"Display ad",4},
				{"Product listing ad",5},
				{"Mobile ad",6},
				{"Local business ad",7},
				{"Third party ad",8},
				{"Other",9}
			};
			GoogleMeasuresDic = new Dictionary<string, string>()
			{
				{"Lead","Leads"},
				{"Signup","Signups"},
				{"Purchase","Purchases"},
				{"Pageview","PageViews"},
				{"Default","Default"},
				{Const.ConversionOnePerClick,"TotalConversionsOnePerClick"},
				{Const.ConversionManyPerClick,"TotalConversionsManyPerClick"}
			};
		}

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			#region Getting Keywords Data
			Dictionary<string,double> _totals=new Dictionary<string,double>();
			DeliveryFile _keyWordsFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT]];
			string[] requiredHeaders = new string[1];
			requiredHeaders[0] = Const.RequiredHeader;
			var _keywordsReader = new CsvDynamicReader(_keyWordsFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
			_keywordsReader.MatchExactColumns = false;
			Dictionary<string, KeywordTarget> _keywordsData = new Dictionary<string, KeywordTarget>();

			using (_keywordsReader)
			{
				while (_keywordsReader.Read())
				{
					if (_keywordsReader.Current[Const.KeywordIdFieldName] == Const.EOF)
						break;
					KeywordPrimaryKey keywordPrimaryKey = new KeywordPrimaryKey()
					{
						KeywordId = Convert.ToInt64(_keywordsReader.Current[Const.KeywordIdFieldName]),
						AdgroupId = Convert.ToInt64(_keywordsReader.Current[Const.AdGroupIdFieldName]),
						CampaignId = Convert.ToInt64(_keywordsReader.Current[Const.CampaignIdFieldName])

					};
					KeywordTarget keyword = new KeywordTarget()
					{
						OriginalID = _keywordsReader.Current[Const.KeywordIdFieldName],
						Keyword = _keywordsReader.Current[Const.KeywordFieldName]
						
					};
					
					keyword.QualityScore = Convert.ToString(_keywordsReader.Current[Const.QualityScoreFieldName]);
					string matchType = _keywordsReader.Current[Const.MatchTypeFieldName];
					keyword.MatchType = (KeywordMatchType)Enum.Parse(typeof(KeywordMatchType), matchType, true);
					
					//Setting Tracker for Keyword
					if (!String.IsNullOrWhiteSpace(Convert.ToString(_keywordsReader.Current[Const.DestUrlFieldName])))
					{
						keyword.DestinationUrl = Convert.ToString(_keywordsReader.Current[Const.DestUrlFieldName]);
						SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, keyword.DestinationUrl);
						if (tracker != null)
							keyword.Segments[Segment.TrackerSegment] = tracker;
					}
					_keywordsData.Add(keywordPrimaryKey.ToString(), keyword);
				}
			}
			#endregion
			
			#region Getting Placements Data

			DeliveryFile _PlacementsFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT]];
			var _PlacementsReader = new CsvDynamicReader(_PlacementsFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
			Dictionary<string, PlacementTarget> _placementsData = new Dictionary<string, PlacementTarget>();

			using (_PlacementsReader)
			{
				while (_PlacementsReader.Read())
				{
					if (_PlacementsReader.Current[Const.KeywordIdFieldName] == Const.EOF)
						break;
					KeywordPrimaryKey placementPrimaryKey = new KeywordPrimaryKey()
					{
						KeywordId = Convert.ToInt64(_PlacementsReader.Current[Const.KeywordIdFieldName]),
						AdgroupId = Convert.ToInt64(_PlacementsReader.Current[Const.AdGroupIdFieldName]),
						CampaignId = Convert.ToInt64(_PlacementsReader.Current[Const.CampaignIdFieldName])
					};
					PlacementTarget placement = new PlacementTarget()
					{
						OriginalID = _PlacementsReader.Current[Const.KeywordIdFieldName],
						Placement = _PlacementsReader.Current[Const.PlacementFieldName],
						PlacementType = PlacementType.Managed
					};
					//Setting Tracker for placment
					if (!String.IsNullOrWhiteSpace(Convert.ToString(_PlacementsReader.Current[Const.DestUrlFieldName])))
					{
						placement.DestinationUrl = Convert.ToString(_PlacementsReader.Current[Const.DestUrlFieldName]);
						SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, placement.DestinationUrl);
						if (tracker != null)
							placement.Segments[Segment.TrackerSegment] = tracker;
					}
					_placementsData.Add(placementPrimaryKey.ToString(), placement);
				}
			}
			#endregion

			#region Getting Conversions Data
			//Get Ads Conversion ( for ex. signup , purchase )

			DeliveryFile _conversionFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv"];
			var _conversionsReader = new CsvDynamicReader(_conversionFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
			Dictionary<string, Dictionary<string, long>> importedAdsWithConv = new Dictionary<string, Dictionary<string, long>>();

			using (_conversionsReader)
			{
				while (_conversionsReader.Read())
				{
					if (_conversionsReader.Current[Const.AdIDFieldName] == Const.EOF) // if end of report
						break;
					string conversionKey = String.Format("{0}#{1}", _conversionsReader.Current[Const.AdIDFieldName], _conversionsReader.Current[Const.KeywordIdFieldName]);
					Dictionary<string, long> conversionDic = new Dictionary<string, long>();

					if (!importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
					{
						//ADD conversionKey to importedAdsWithConv
						//than add conversion field to importedAdsWithConv : <conversion name , conversion value>
						Dictionary<string, long> conversion = new Dictionary<string, long>();
						conversion.Add(Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurpose]), Convert.ToInt64(_conversionsReader.Current[Const.ConversionManyPerClick]));
						importedAdsWithConv.Add(conversionKey, conversion);
					}
					else // if Key exists
					{
						// if current add already has current conversion type than add value to the current type
						if (!conversionDic.ContainsKey(Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurpose])))
							conversionDic.Add(Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurpose]), Convert.ToInt64(_conversionsReader.Current[Const.ConversionManyPerClick]));
						// else create new conversion type and add the value
						else
							conversionDic[Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurpose])] += Convert.ToInt64(_conversionsReader.Current[Const.ConversionManyPerClick]);
					}
				}
			}
			#endregion

			#region Getting Ads Data

			DeliveryFile _adPerformanceFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT]];
			var _adsReader = new CsvDynamicReader(_adPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
			Dictionary<string, Ad> importedAds = new Dictionary<string, Ad>();
			using (var session = new AdMetricsImportManager(this.Instance.InstanceID))
			{
				//session.Begin(false);
				session.BeginImport(this.Delivery);
				

				using (_adsReader)
				{
					while (_adsReader.Read())
					{

						// Adding totals line for validation (checksum)
						if (_adsReader.Current[Const.AdIDFieldName] == Const.EOF)
						{
							foreach (KeyValuePair<string, Measure> measure in session.Measures)
							{
								if (!measure.Value.Options.HasFlag(MeasureOptions.IntegrityCheckRequired))
									continue;

								switch (measure.Key)
								{
									case Measure.Common.Clicks: _totals[Measure.Common.Clicks] = Convert.ToInt64(_adsReader.Current.Clicks); break;
									case Measure.Common.Cost: _totals[Measure.Common.Cost] = (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000; break;
									case Measure.Common.Impressions: _totals[Measure.Common.Impressions] = Convert.ToInt64(_adsReader.Current.Impressions); break;
								}
							}
							break;
						}

						AdMetricsUnit adMetricsUnit = new AdMetricsUnit();
						Ad ad;

						string adId = _adsReader.Current[Const.AdIDFieldName];
						if (!importedAds.ContainsKey(adId))
						{
							ad = new Ad();
							ad.OriginalID = adId;
							

							//Ad Type
							string adTypeKey = Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]);
							ad.ExtraFields[AdType] = GoogleAdTypeDic[adTypeKey];
							ad.Creatives.Add(new TextCreative { TextType = TextCreativeType.DisplayUrl, Text = _adsReader.Current[Const.DisplayURLFieldName] });

							////Setting Tracker for Ad
							if (!String.IsNullOrWhiteSpace(_adsReader.Current[Const.DestUrlFieldName]))
							{
								ad.DestinationUrl = _adsReader.Current[Const.DestUrlFieldName];
								SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, _adsReader.Current[Const.DestUrlFieldName]);
								if (tracker != null)
									ad.Segments[Segment.TrackerSegment] = tracker;
							}
							
							ad.Campaign = new Campaign()
							{
								OriginalID = _adsReader.Current[Const.CampaignIdFieldName],
								Name = _adsReader.Current[Const.CampaignFieldName],
								Channel = new Channel() { ID = 1 },
								Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_adPerformanceFile.Parameters["clientCustomerId"] }
							};

							//Image Type > Create Image
							if (String.Equals(Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]), "Image ad"))
							{
								string adNameField = _adsReader.Current[Const.AdFieldName];
								string[] imageParams = adNameField.Trim().Split(new Char[] { ':', ';' }); // Ad name: 468_60_Test7options_Romanian.swf; 468 x 60
								ad.Name = imageParams[1].Trim();
								ad.Creatives.Add(new ImageCreative()
								{
									ImageUrl = imageParams[1].Trim(),
									ImageSize = imageParams[2].Trim()
								});
							}

							else //Text ad or Display ad
							{
								ad.Name = _adsReader.Current[Const.AdFieldName];
								ad.Creatives.Add(new TextCreative
								{
									TextType = TextCreativeType.Title,
									Text = _adsReader.Current.Ad,
								});
								ad.Creatives.Add(new TextCreative
								{
									TextType = TextCreativeType.Body,
									Text = _adsReader.Current["Description line 1"],
									Text2 = _adsReader.Current["Description line 2"]
								});
							}

							//Insert Adgroup 
							ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
							{
								Value = _adsReader.Current[Const.AdGroupFieldName],
								OriginalID = _adsReader.Current[Const.AdGroupIdFieldName]
							};

							//Insert Network Type Display Network / Search Network
							string networkType = Convert.ToString(_adsReader.Current[Const.NetworkFieldName]);

							if (networkType.Equals(Const.GoogleSearchNetwork))
								networkType = Const.SystemSearchNetwork;
							else if (networkType.Equals(Const.GoogleDisplayNetwork))
								networkType = Const.SystemDisplayNetwork;

							ad.ExtraFields[NetworkType] = networkType;

							importedAds.Add(adId, ad);
							session.ImportAd(ad);
						}
						else ad = importedAds[adId];
						adMetricsUnit.Ad = ad;

						//SERACH KEYWORD IN KEYWORD/ Placements  Dictionary
						KeywordPrimaryKey kwdKey = new KeywordPrimaryKey()
						{
							AdgroupId = Convert.ToInt64(_adsReader.Current[Const.AdGroupIdFieldName]),
							KeywordId = Convert.ToInt64(_adsReader.Current[Const.KeywordIdFieldName]),
							CampaignId = Convert.ToInt64(_adsReader.Current[Const.CampaignIdFieldName])
						};

						if (ad.ExtraFields[NetworkType].Equals(Const.SystemSearchNetwork))
						{
							KeywordTarget kwd = new KeywordTarget();
							try
							{
								kwd = _keywordsData[kwdKey.ToString()];
							}
							catch (Exception)
							{
								//Creating Error file with all Keywords primary keys that doesnt exists in keyword report.
								//_keywordErrorFile.Open();
								//_keywordErrorFile.AppendToFile(kwdKey.ToList());

								//Creating KWD with OriginalID , since the KWD doesnt exists in KWD report.
								kwd = new KeywordTarget { OriginalID = Convert.ToString(_adsReader.Current[Const.KeywordIdFieldName]) };
							}
							//INSERTING KEYWORD INTO METRICS
							adMetricsUnit.TargetMatches = new List<Target>();
							adMetricsUnit.TargetMatches.Add(kwd);
						}
						else
						{
							PlacementTarget placement = new PlacementTarget();
							try
							{
								placement = _placementsData[kwdKey.ToString()];
							}
							catch (Exception)
							{
								placement.OriginalID = Convert.ToString(_adsReader.Current[Const.KeywordIdFieldName]);
								placement.PlacementType = PlacementType.Automatic;
								placement.Placement = Const.AutoDisplayNetworkName;
							}
							//INSERTING KEYWORD INTO METRICS
							adMetricsUnit.TargetMatches = new List<Target>();
							adMetricsUnit.TargetMatches.Add(placement);
						}

						//INSERTING METRICS DATA
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Clicks]] = Convert.ToInt64(_adsReader.Current.Clicks);
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Cost]] = (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000;
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Impressions]] = Convert.ToInt64(_adsReader.Current.Impressions);
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.AveragePosition]] = Convert.ToDouble(_adsReader.Current[Const.AvgPosition]);
						adMetricsUnit.MeasureValues[session.Measures[GoogleMeasuresDic[Const.ConversionOnePerClick]]] = Convert.ToDouble( _adsReader.Current[Const.ConversionOnePerClick]);
						adMetricsUnit.MeasureValues[session.Measures[GoogleMeasuresDic[Const.ConversionManyPerClick]]] = Convert.ToDouble( _adsReader.Current[Const.ConversionManyPerClick]);


						//Inserting conversion values
						string conversionKey = String.Format("{0}#{1}", ad.OriginalID, _adsReader.Current[Const.KeywordIdFieldName]);
						Dictionary<string, long> conversionDic = new Dictionary<string, long>();
						if (importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
						{
							foreach (var pair in conversionDic)
								adMetricsUnit.MeasureValues[session.Measures[GoogleMeasuresDic[pair.Key]]] = pair.Value;
						}

						adMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
						adMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

						adMetricsUnit.Currency = new Currency
						{
							Code = Convert.ToString(_adsReader.Current.Currency)
						};
						session.ImportMetrics(adMetricsUnit);
					}
					session.HistoryEntryParameters.Add("Totals", _totals);
					session.EndImport();
				}

			}
			#endregion
			
			return Core.Services.ServiceOutcome.Success;
		}

		private static class Const
		{
			public const string RequiredHeader = "Keyword ID";
			public const string EOF = "Total";

			public const string KeywordIdFieldName = "Keyword ID";
			public const string KeywordFieldName = "Keyword";
			public const string AvgPosition = "Avg. position";

			public const string ConversionManyPerClick = "Conv. (many-per-click)";
			public const string ConversionOnePerClick = "Conv. (1-per-click)";
			//public const string ConversionManyPerClick = "Conv. rate (many-per-click)";
			public const string TotalConversionsOnePerClick = "TotalConversionsOnePerClick";
			public const string ConversionTrackingPurpose = "Conversion tracking purpose";

			public const string AdGroupIdFieldName = "Ad group ID";
			public const string AdGroupFieldName = "Ad group";

			public const string CampaignIdFieldName = "Campaign ID";
			public const string CampaignFieldName = "Campaign";

			public const string QualityScoreFieldName = "Quality score";
			public const string MatchTypeFieldName = "Match type";
			public const string PlacementFieldName = "Placement";

			public const string AdIDFieldName = "Ad ID";
			public const string AdTypeFieldName = "Ad type";
			public const string AdFieldName = "Ad";
			public const string DisplayURLFieldName = "Display URL";
			public const string DestUrlFieldName = "Destination URL";

			public const string NetworkFieldName = "Network";
			public const string GoogleSearchNetwork = "Search Network";
			public const string SystemSearchNetwork = "Search Only";
			public const string GoogleDisplayNetwork = "Display Network";
			public const string SystemDisplayNetwork = "Content Only";

			public const string AutoDisplayNetworkName = "Total - content targeting";

		}

		
	}
}


