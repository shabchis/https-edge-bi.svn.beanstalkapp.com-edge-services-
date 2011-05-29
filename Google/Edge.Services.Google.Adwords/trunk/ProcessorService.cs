using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Importing;

namespace Edge.Services.Google.Adwords
{
	class ProcessorService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			// Get Keywords data
			DeliveryFile _keyWordsFile = this.Delivery.Files["KEYWORDS_PERFORMANCE_REPORT"];
			var _keywordsReader = new CsvDynamicReader(_keyWordsFile.Location);
			Dictionary<KeywordPrimaryKey, KeywordTarget> _keywordsData = new Dictionary<KeywordPrimaryKey, KeywordTarget>();

			using (_keywordsReader)
			{
				while (_keywordsReader.Read())
				{
					KeywordPrimaryKey keywordPrimaryKey = new KeywordPrimaryKey()
					{
						KeywordId = _keywordsReader.Current.Keyword_ID,
						AdgroupId = _keywordsReader.Current.Ad_group_ID
					};
					KeywordTarget keyword = new KeywordTarget()
					{
						OriginalID = _keywordsReader.Current.Keyword_ID,
						Keyword = _keywordsReader.Current.Keyword,
						MatchType = _keywordsReader.Current.Match_type
					};

					_keywordsData.Add(keywordPrimaryKey, keyword);

					// Get Ads data.
					DeliveryFile _adPerformanceFile = this.Delivery.Files["AD_PERFORMANCE_REPORT"];
					var _adsReader = new CsvDynamicReader(_adPerformanceFile.Location);

					using (var session = new AdDataImportSession(this.Delivery))
					{
						session.Begin(true);
						using (_adsReader)
						{
							while (_adsReader.Read())
							{
								AdMetricsUnit adMetricsUnit = new AdMetricsUnit();

								adMetricsUnit.Ad = new Ad()
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
									}
								};
								//TODO: INSERT ADGROUP AS A SEGMENT
								//TODO: INSERT KEYWORD INTO METRICS- SERACH KEYWORD IN KEYWORD DICTIONARY


								adMetricsUnit.Clicks = Convert.ToInt64(_adsReader.Current.Clicks);
								adMetricsUnit.Cost = Convert.ToDouble(_adsReader.Current.Cost);
								adMetricsUnit.Impressions = Convert.ToInt64(_adsReader.Current.impressions);
								adMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
								//adMetricsUnit.TargetMatches.
							}
						}

					}

				}
			}

			return Core.Services.ServiceOutcome.Success;
		}
	}
}
