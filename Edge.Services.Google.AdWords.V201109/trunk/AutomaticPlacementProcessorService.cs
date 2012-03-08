using System;
using System.Collections.Generic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using GA = Google.Api.Ads.AdWords.v201109;
using System.IO;
using Edge.Services.TargetingMetrics;

namespace Edge.Services.Google.AdWords
{
	class AutomaticPlacementProcessorService : PipelineService
	{
		static Dictionary<string, string> GoogleMeasuresDic;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			#region Getting Placements Data

			string[] requiredHeaders = new string[1];
			requiredHeaders[0] = Const.RequiredHeader;
			DeliveryFile _placementsPerformanceFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT]];
			var _placementReader = new CsvDynamicReader(_placementsPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);

			//Dictionary<string, Ad> importedAds = new Dictionary<string, Ad>();
			using (var session = new TargetingMetricsImportManager(this.Instance.InstanceID))
			{
				//session.Begin(false);
				session.BeginImport(this.Delivery);

				foreach (KeyValuePair<string, Measure> measure in session.Measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						_totals.Add(measure.Key, 0);
					}

				}

				using (_placementReader)
				{
					while (_placementReader.Read())
					{

						// Adding totals line for validation (checksum)
						if (_placementReader.Current[Const.AdIDFieldName] == Const.EOF)
						{
							foreach (KeyValuePair<string, Measure> measure in session.Measures)
							{
								if (!measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
									continue;

								switch (measure.Key)
								{
									case Measure.Common.Clicks: _totals[Measure.Common.Clicks] = Convert.ToInt64(_placementReader.Current.Clicks); break;
									case Measure.Common.Cost: _totals[Measure.Common.Cost] = (Convert.ToDouble(_placementReader.Current.Cost)) / 1000000; break;
									case Measure.Common.Impressions: _totals[Measure.Common.Impressions] = Convert.ToInt64(_placementReader.Current.Impressions); break;
								}
							}
							break;
						}
						PlacementTarget target = new PlacementTarget();

						TargetingMetricsUnit targetsMetricsUnit = new TargetingMetricsUnit();

						target.Campaign = new Campaign()
						{
							OriginalID = _placementReader.Current[Const.CampaignIdFieldName],
							Name = _placementReader.Current[Const.CampaignFieldName],
							Channel = new Channel() { ID = 1 },
							Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_placementsPerformanceFile.Parameters["AdwordsClientID"] }
						};

						//Image Type > Create Image
						if (String.Equals(Convert.ToString(_placementReader.Current[Const.AdTypeFieldName]), "Image ad"))
						{
							string adNameField = _placementReader.Current[Const.AdFieldName];
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
							ad.Name = _placementReader.Current[Const.AdFieldName];
							ad.Creatives.Add(new TextCreative
							{
								TextType = TextCreativeType.Title,
								Text = _placementReader.Current.Ad,
							});
							ad.Creatives.Add(new TextCreative
							{
								TextType = TextCreativeType.Body,
								Text = _placementReader.Current["Description line 1"],
								Text2 = _placementReader.Current["Description line 2"]
							});
						}

						//Insert Adgroup 
						ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
						{
							Value = _placementReader.Current[Const.AdGroupFieldName],
							OriginalID = _placementReader.Current[Const.AdGroupIdFieldName]
						};

						//Insert Network Type Display Network / Search Network
						string networkType = Convert.ToString(_placementReader.Current[Const.NetworkFieldName]);

						if (networkType.Equals(Const.GoogleSearchNetwork))
							networkType = Const.SystemSearchNetwork;
						else if (networkType.Equals(Const.GoogleDisplayNetwork))
							networkType = Const.SystemDisplayNetwork;

						ad.ExtraFields[NetworkType] = networkType;

						importedAds.Add(adId, ad);
						session.ImportAd(ad);

						targetsMetricsUnit.Ad = ad;

						//SERACH KEYWORD IN KEYWORD/ Placements  Dictionary
						KeywordPrimaryKey kwdKey = new KeywordPrimaryKey()
						{
							AdgroupId = Convert.ToInt64(_placementReader.Current[Const.AdGroupIdFieldName]),
							KeywordId = Convert.ToInt64(_placementReader.Current[Const.KeywordIdFieldName]),
							CampaignId = Convert.ToInt64(_placementReader.Current[Const.CampaignIdFieldName])
						};

						//Check if keyword file contains this kwdkey.
						if (kwdKey.KeywordId != Convert.ToInt64(this.Delivery.Parameters["KeywordContentId"]) && _keywordsData.ContainsKey(kwdKey.ToString()))
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
								kwd = new KeywordTarget { OriginalID = Convert.ToString(_placementReader.Current[Const.KeywordIdFieldName]) };
							}
							//INSERTING KEYWORD INTO METRICS
							targetsMetricsUnit.TargetMatches = new List<Target>();
							targetsMetricsUnit.TargetMatches.Add(kwd);
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
								placement.OriginalID = Convert.ToString(_placementReader.Current[Const.KeywordIdFieldName]);
								placement.PlacementType = PlacementType.Automatic;
								placement.Placement = Const.AutoDisplayNetworkName;
							}
							//INSERTING KEYWORD INTO METRICS
							targetsMetricsUnit.TargetMatches = new List<Target>();
							targetsMetricsUnit.TargetMatches.Add(placement);
						}

						//INSERTING METRICS DATA
						targetsMetricsUnit.MeasureValues[session.Measures[Measure.Common.Clicks]] = Convert.ToInt64(_placementReader.Current.Clicks);
						targetsMetricsUnit.MeasureValues[session.Measures[Measure.Common.Cost]] = (Convert.ToDouble(_placementReader.Current.Cost)) / 1000000;
						targetsMetricsUnit.MeasureValues[session.Measures[Measure.Common.Impressions]] = Convert.ToInt64(_placementReader.Current.Impressions);
						targetsMetricsUnit.MeasureValues[session.Measures[Measure.Common.AveragePosition]] = Convert.ToDouble(_placementReader.Current[Const.AvgPosition]);
						targetsMetricsUnit.MeasureValues[session.Measures[GoogleMeasuresDic[Const.ConversionOnePerClick]]] = Convert.ToDouble(_placementReader.Current[Const.ConversionOnePerClick]);
						targetsMetricsUnit.MeasureValues[session.Measures[GoogleMeasuresDic[Const.ConversionManyPerClick]]] = Convert.ToDouble(_placementReader.Current[Const.ConversionManyPerClick]);

						//Inserting conversion values
						string conversionKey = String.Format("{0}#{1}", ad.OriginalID, _placementReader.Current[Const.KeywordIdFieldName]);
						Dictionary<string, long> conversionDic = new Dictionary<string, long>();
						if (importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
						{
							foreach (var pair in conversionDic)
							{

								if (GoogleMeasuresDic.ContainsKey(pair.Key))
								{
									//if (adMetricsUnit.MeasureValues.ContainsKey(session.Measures[GoogleMeasuresDic[pair.Key]]))
									//{
									targetsMetricsUnit.MeasureValues[session.Measures[GoogleMeasuresDic[pair.Key]]] = pair.Value;
									//}
								}
							}
						}

						targetsMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
						targetsMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

						targetsMetricsUnit.Currency = new Currency
						{
							Code = Convert.ToString(_placementReader.Current.Currency)
						};
						session.ImportMetrics(targetsMetricsUnit);
					}
					session.HistoryEntryParameters.Add(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, _totals);
					session.EndImport();
				}

			}
			#endregion

			return Core.Services.ServiceOutcome.Success;
		}
	}
}


