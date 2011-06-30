using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Importing;
using GA = Google.Api.Ads.AdWords.v201101;
using System.IO;

namespace Edge.Services.Google.Adwords
{
	class ProcessorService : PipelineService
	{
		ErrorFile _keywordErrorFile = new ErrorFile("Errors_KeywordPrimaryKey", new List<string> { "AdgroupId", "KeywordId", "CampaignId" }, @"D:\");

		static ExtraField NetworkType = new ExtraField() { ColumnIndex = 1, Name = "NetworkType" };
		static ExtraField KeywordScore = new ExtraField() { ColumnIndex = 1, Name = "KeywordScore" };

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{





			// Get Keywords data
			DeliveryFile _keyWordsFile = this.Delivery.Files[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT.ToString()];
			string[] requiredHeaders = new string[1];
			requiredHeaders[0] = Const.RequiredHeader;
			var _keywordsReader = new CsvDynamicReader(_keyWordsFile.OpenContents(Path.GetFileNameWithoutExtension(_keyWordsFile.Location), FileFormat.GZip), requiredHeaders);
			_keywordsReader.MatchExactColumns = false;
			Dictionary<string, KeywordTarget> _keywordsData = new Dictionary<string, KeywordTarget>();

			using (_keywordsReader)
			{
				while (_keywordsReader.Read())
				{
					if (_keywordsReader.Current["Keyword ID"] == Const.EOF)
						break;
					KeywordPrimaryKey keywordPrimaryKey = new KeywordPrimaryKey()
					{
						KeywordId = Convert.ToInt64(_keywordsReader.Current["Keyword ID"]),
						AdgroupId = Convert.ToInt64(_keywordsReader.Current["Ad group ID"]),
						CampaignId = Convert.ToInt64(_keywordsReader.Current["Campaign ID"])

					};
					KeywordTarget keyword = new KeywordTarget()
					{
						OriginalID = _keywordsReader.Current["Keyword ID"],
						Keyword = _keywordsReader.Current["Keyword"]

					};
					//keyword.ExtraFields[KeywordScore] = _keywordsReader.Current["Quality score"];
					string matchType = _keywordsReader.Current["Match type"];
					keyword.MatchType = (KeywordMatchType)Enum.Parse(typeof(KeywordMatchType), matchType, true);
					if (!String.IsNullOrEmpty(Convert.ToString(_keywordsReader.Current[Const.KeyWordDestUrlField])))
						keyword.DestinationUrl = Convert.ToString(_keywordsReader.Current[Const.KeyWordDestUrlField]);
					_keywordsData.Add(keywordPrimaryKey.ToString(), keyword);
				}
			}

			// Get Placements data
			DeliveryFile _PlacementsFile = this.Delivery.Files[GA.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT.ToString()];
			var _PlacementsReader = new CsvDynamicReader(_PlacementsFile.OpenContents(Path.GetFileNameWithoutExtension(_PlacementsFile.Location), FileFormat.GZip), requiredHeaders);
			Dictionary<string, PlacementTarget> _placementsData = new Dictionary<string, PlacementTarget>();

			using (_PlacementsReader)
			{
				while (_PlacementsReader.Read())
				{
					if (_PlacementsReader.Current["Keyword ID"] == Const.EOF)
						break;
					KeywordPrimaryKey placementPrimaryKey = new KeywordPrimaryKey()
					{
						KeywordId = Convert.ToInt64(_PlacementsReader.Current["Keyword ID"]),
						AdgroupId = Convert.ToInt64(_PlacementsReader.Current["Ad group ID"]),
						CampaignId = Convert.ToInt64(_PlacementsReader.Current["Campaign ID"])

					};
					PlacementTarget placement = new PlacementTarget()
					{
						OriginalID = _PlacementsReader.Current["Keyword ID"],
						DestinationUrl = _PlacementsReader.Current["Destination URL"],
						Placement = _PlacementsReader.Current["Placement"]
					};
					_placementsData.Add(placementPrimaryKey.ToString(), placement);
				}
			}


			//Get Ads Conversion ( for ex. signup , purchase )

			DeliveryFile _conversionFile = this.Delivery.Files["AD_PERFORMANCE_REPORT_(Conversion)"];
			var _conversionsReader = new CsvDynamicReader(_conversionFile.OpenContents(Path.GetFileNameWithoutExtension(_conversionFile.Location), FileFormat.GZip), requiredHeaders);
			Dictionary<string, Dictionary<string, long>> importedAdsWithConv = new Dictionary<string, Dictionary<string, long>>();

			using (_conversionsReader)
			{
				while (_conversionsReader.Read())
				{
					if (_conversionsReader.Current["Ad ID"] == Const.EOF) // if end of report
						break;
					string conversionKey = String.Format("{0}#{1}", _conversionsReader.Current["Ad ID"], _conversionsReader.Current["Keyword ID"]);
					Dictionary<string, long> conversionDic = new Dictionary<string, long>();

					if (!importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
					{
						//ADD conversionKey to importedAdsWithConv
						//than add conversion field to importedAdsWithConv : <conversion name , conversion value>
						Dictionary<string, long> conversion = new Dictionary<string, long>();
						conversion.Add(Convert.ToString(_conversionsReader.Current["Conversion tracking purpose"]), Convert.ToInt64(_conversionsReader.Current["Conv. (many-per-click)"]));
						importedAdsWithConv.Add(conversionKey, conversion);
					}
					else // if Key exists
					{
						// if current add already has current conversion type than add value to the current type
						if (!conversionDic.ContainsKey(Convert.ToString(_conversionsReader.Current["Conversion tracking purpose"])))
							conversionDic.Add(Convert.ToString(_conversionsReader.Current["Conversion tracking purpose"]), Convert.ToInt64(_conversionsReader.Current[Const.ConversionValueFieldName]));
						// else create new conversion type and add the value
						else
							conversionDic[Convert.ToString(_conversionsReader.Current["Conversion tracking purpose"])] += Convert.ToInt64(_conversionsReader.Current[Const.ConversionValueFieldName]);
					}
				}

			}

			// Get Ads data.
			DeliveryFile _adPerformanceFile = this.Delivery.Files["AD_PERFORMANCE_REPORT"];
			var _adsReader = new CsvDynamicReader(_adPerformanceFile.OpenContents(Path.GetFileNameWithoutExtension(_keyWordsFile.Location), FileFormat.GZip), requiredHeaders);
			Dictionary<string, Ad> importedAds = new Dictionary<string, Ad>();
			using (var session = new AdDataImportSession(this.Delivery))
			{
				session.Begin(false);

				using (_adsReader)
				{
					while (_adsReader.Read())
					{

						if (_adsReader.Current["Ad ID"] == Const.EOF)
							break;

						AdMetricsUnit adMetricsUnit = new AdMetricsUnit();

						Ad ad;


						// (1)ADD NEW AD TO ADS DIC 
						// (2)CHECK IF AD ALREADY EXISTS IN DIC 
						// (3)IF NOT IMPORT AD

						string adId = _adsReader.Current["Ad ID"];
						if (!importedAds.ContainsKey(adId))
						{
							ad = new Ad();
							ad.OriginalID = adId;
							ad.DestinationUrl = _adsReader.Current["Destination URL"];
							
							// ad tracker
							SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, ad.DestinationUrl);
							if (tracker != null)
								ad.Segments[Segment.TrackerSegment] = tracker;

							ad.Campaign = new Campaign()
							{
								OriginalID = _adsReader.Current["Campaign ID"],
								Name = _adsReader.Current["Campaign"],
								Channel = new Channel() { ID = 1 },
								Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_adPerformanceFile.Parameters["Email"] }
							};

							//if Image 
							if (String.Equals(Convert.ToString(_adsReader.Current["Ad type"]), "Image ad"))
							{
								string adNameField = _adsReader.Current["Ad"];
								string[] imageParams = adNameField.Trim().Split(new Char[] { ':', ';' }); // Ad name: 468_60_Test7options_Romanian.swf; 468 x 60
								ad.Creatives.Add(new ImageCreative()
								{
									Name = imageParams[1],
									ImageUrl = imageParams[1],
									ImageSize = imageParams[2]
								});

							}
							else //if text ad or display ad
							{
								ad.Name = _adsReader.Current["Ad"];
								ad.Creatives.Add(new TextCreative
								{
									TextType = TextCreativeType.Title,
									Text = _adsReader.Current.Ad
								});
								ad.Creatives.Add(new TextCreative
								{
									TextType = TextCreativeType.Body,
									Text = _adsReader.Current["Description line 1"],
									Text2 = _adsReader.Current["Description line 2"]
								});
								ad.Creatives.Add(new TextCreative { TextType = TextCreativeType.DisplayUrl, Text = _adsReader.Current["Display URL"] });
							}

							//Insert adgroup 
							ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
							{
								Value = _adsReader.Current["Ad group"],
								OriginalID = _adsReader.Current["Ad group ID"]
							};

							//Insert Network Type Display Network \ Search Network
							string networkType = Convert.ToString(_adsReader.Current["Network"]);
							if (networkType.Equals("Search Network"))
								networkType = "Search Only";
							ad.ExtraFields[NetworkType] = networkType;

							importedAds.Add(adId, ad);
							session.ImportAd(ad);
						}
						else ad = importedAds[adId];
						adMetricsUnit.Ad = ad;

						//SERACH KEYWORD IN KEYWORD/ Placements  DICTIONARY
						KeywordPrimaryKey kwdKey = new KeywordPrimaryKey()
						{
							AdgroupId = Convert.ToInt64(_adsReader.Current["Ad group ID"]),
							KeywordId = Convert.ToInt64(_adsReader.Current["Keyword ID"]),
							CampaignId = Convert.ToInt64(_adsReader.Current["Campaign ID"])
						};

						//Search Network
						Target _target;
						//string targrtOriginalId = "-1";
						if (ad.ExtraFields[NetworkType].Equals("Search Network"))
						{
							_target = new KeywordTarget();
							try
							{
								_target = _keywordsData[kwdKey.ToString()];
							}
							catch (Exception)
							{
								//Creating Error file with all Keywords primary keys that doesnt exists in keyword report.
								_keywordErrorFile.Open();
								_keywordErrorFile.AppendToFile(kwdKey.ToList());

								//Creating KWD with OriginalID , since the KWD doesnt exists in KWD report.
								_target = new KeywordTarget { OriginalID = Convert.ToString(_adsReader.Current["Keyword ID"]) };
							}
							//INSERTING KEYWORD INTO METRICS
							adMetricsUnit.TargetMatches = new List<Target>();
							adMetricsUnit.TargetMatches.Add(_target);
						}
						else // (ad.ExtraFields[NetworkType].Equals("Display Network"))
						{
							_target = new PlacementTarget();
							try
							{
								_target = _placementsData[kwdKey.ToString()];
							}
							catch (Exception)
							{
								_target.OriginalID = Convert.ToString(_adsReader.Current["Keyword ID"]);
							}
							//INSERTING KEYWORD INTO METRICS
							adMetricsUnit.TargetMatches = new List<Target>();
							adMetricsUnit.TargetMatches.Add(_target);
						}

						//INSERTING METRICS DATA
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Clicks]] = Convert.ToInt64(_adsReader.Current.Clicks);
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Cost]] = Convert.ToDouble(_adsReader.Current.Cost);
						adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Impressions]] = Convert.ToInt64(_adsReader.Current.Impressions);

						//adMetricsUnit.MeasureValues[session.Measures[Measure.Common.AveragePosition]] = Convert.ToString(_adsReader.Current[Const.AvgPosition]);
						//adMetricsUnit.MeasureValues[session.Measures[Const.TotalConversionsOnePerClick]] = Convert.ToString(_adsReader.Current[Const.ConversionOnePerClick]);

						//inserting conversion values
						string conversionKey = String.Format("{0}#{1}", ad.OriginalID, _target.OriginalID);
						Dictionary<string, long> conversionDic = new Dictionary<string, long>();
						if (importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
						{
							foreach (var pair in conversionDic)
							{
								try
								{
									adMetricsUnit.MeasureValues[session.Measures[pair.Key]] = pair.Value;
								}
								catch (KeyNotFoundException)
								{
									Console.WriteLine(string.Format("{0} measure doesn't exists in measures table", pair.Key));
								}
							}
						}

						adMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
						adMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

						adMetricsUnit.Currency = new Currency
						{
							Code = Convert.ToString(_adsReader.Current.Currency)
						};
						session.ImportMetrics(adMetricsUnit);
					}
				}

			}


			return Core.Services.ServiceOutcome.Success;
		}

		private static class Const
		{
			public const string ConversionValueFieldName = "Conv. (many-per-click)";
			public const string ConversionOnePerClick = "Conv. rate (1-per-click)";
			public const string ConversionManyPerClick = "Conv. rate (many-per-click)";
			public const string AvgPosition = "Avg. position";
			public const string RequiredHeader = "Keyword ID";
			public const string EOF = "Total";
			public const string TotalConversionsOnePerClick = "TotalConversionsOnePerClick";
			public const string KeyWordDestUrlField = "Destination URL";
		}
	}
}
