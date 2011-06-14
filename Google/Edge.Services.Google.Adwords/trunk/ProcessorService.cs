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

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			// Get Keywords data
			DeliveryFile _keyWordsFile = this.Delivery.Files[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT.ToString()];
			string[] requiredHeaders = new string[1];
			requiredHeaders[0] = "Keyword ID";
			var _keywordsReader = new CsvDynamicReader(_keyWordsFile.OpenContents(Path.GetFileNameWithoutExtension(_keyWordsFile.Location), FileFormat.GZip), requiredHeaders);
			_keywordsReader.MatchExactColumns = false;
			Dictionary<string, KeywordTarget> _keywordsData = new Dictionary<string, KeywordTarget>();

			using (_keywordsReader)
			{
				while (_keywordsReader.Read())
				{
					if (_keywordsReader.Current.Keyword_ID == "Total")
						break;
					KeywordPrimaryKey keywordPrimaryKey = new KeywordPrimaryKey()
					{
						KeywordId = Convert.ToInt64(_keywordsReader.Current.Keyword_ID),
						AdgroupId = Convert.ToInt64(_keywordsReader.Current.Ad_group_ID),
						CampaignId = Convert.ToInt64(_keywordsReader.Current.Campaign_ID)

					};
					KeywordTarget keyword = new KeywordTarget()
					{
						OriginalID = _keywordsReader.Current.Keyword_ID,
						Keyword = _keywordsReader.Current.Keyword

					};
					string matchType = _keywordsReader.Current.Match_type;
					keyword.MatchType = (KeywordMatchType)Enum.Parse(typeof(KeywordMatchType), matchType, true);
					_keywordsData.Add(keywordPrimaryKey.ToString(), keyword);
				}
			}

				// Get Ads data.
				DeliveryFile _adPerformanceFile = this.Delivery.Files["AD_PERFORMANCE_REPORT"];
				var _adsReader = new CsvDynamicReader(_adPerformanceFile.OpenContents(Path.GetFileNameWithoutExtension(_keyWordsFile.Location), FileFormat.GZip), requiredHeaders);

				using (var session = new AdDataImportSession(this.Delivery))
				{
					session.Begin(false);
					using (_adsReader)
					{
						while (_adsReader.Read())
						{
							

							Ad ad= new Ad()
							{
								OriginalID = _adsReader.Current.Ad_ID,
								DestinationUrl = _adsReader.Current.Destination_URL,
								Campaign = new Campaign()
								{
									OriginalID = _adsReader.Current.Campaign_ID,
									Name = _adsReader.Current.Campaign,
									Channel = new Channel()
									{
										ID = 1
									},
									Account = new Account { ID = this.Delivery.Account.ID }
								}
							};
							//ad.Creatives.Add ( new TextCreative { Name = "desc1" , TextType =TextCreativeType.Body ,
							//INSERT ADGROUP AS A SEGMENT
							ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
										{
											Value = _adsReader.Current.Ad_group,
											OriginalID = _adsReader.Current.Ad_group_ID
										};

							session.ImportAd(ad);

							AdMetricsUnit adMetricsUnit = new AdMetricsUnit();
							adMetricsUnit.Ad = ad;

							//SERACH KEYWORD IN KEYWORD DICTIONARY
							KeywordPrimaryKey kwdKey = new KeywordPrimaryKey()
							{
								AdgroupId = Convert.ToInt64(_adsReader.Current.Ad_group_ID),
								KeywordId = Convert.ToInt64(_adsReader.Current.Keyword_ID),
								CampaignId = Convert.ToInt64(_adsReader.Current.Campaign_ID)
							};

							KeywordTarget _kwd = new KeywordTarget();
							try
							{
								_kwd = _keywordsData[kwdKey.ToString()];
								
							}
							catch (Exception e)
							{
								//Creating Error file with all Keywords primary keys that doesnt exists in keyword report.
								_keywordErrorFile.Open();
								_keywordErrorFile.AppendToFile(kwdKey.ToList());

								//Creating KWD with OriginalID , since the KWD doesnt exists in KWD report.
								_kwd = new KeywordTarget { OriginalID = Convert.ToString(_adsReader.Current.Keyword_ID) };
							}

							//INSERTING KEYWORD INTO METRICS
							adMetricsUnit.TargetMatches = new List<Target>();
							adMetricsUnit.TargetMatches.Add(_kwd);

							//INSERTING METRICS DATA
							adMetricsUnit.Clicks = Convert.ToInt64(_adsReader.Current.Clicks);
							adMetricsUnit.Cost = Convert.ToDouble(_adsReader.Current.Cost);
							adMetricsUnit.Impressions = Convert.ToInt64(_adsReader.Current.Impressions);
							adMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
							adMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

							//TODO: pull from configuration 
							adMetricsUnit.Currency = new Currency
							{
								Code = "USD"
							};
							session.ImportMetrics(adMetricsUnit);
							

						}
					}

				}
			

			return Core.Services.ServiceOutcome.Success;
		}
	}
}
