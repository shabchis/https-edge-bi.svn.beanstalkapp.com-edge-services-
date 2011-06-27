using System;
using System.Collections.Generic;
using Edge.Core.Data;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Importing;
using Edge.Data.Pipeline.Services;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;
using System.Text.RegularExpressions;


namespace Edge.Services.Microsoft.AdCenter
{
	public class ProcessorService : PipelineService
	{
		Dictionary<string, Campaign> _campaignsCache;

		protected override ServiceOutcome DoPipelineWork()
		{
			// TODO: add checks for delivery state

			DeliveryFile adReport = this.Delivery.Files[Const.Files.AdReport];
			//DeliveryFile keywordReport = this.Delivery.Files[Const.Files.KeywordReport];
			//FileInfo adReportFileInfo = adReport.GetFileInfo();
			//FileInfo keywordReportFileInfo = keywordReport.GetFileInfo();

			//// Some values for progress (guessing)
			//double progress = 0;
			//long totalSize = (long)((adReportFileInfo.TotalBytes + keywordReportFileInfo.TotalBytes) * 1.1); // add 10% to the total size, just in case
			//const long adSize = 512; // roughly 1/2 KB per row
			//const long kwSize = 1024; // rougly 1 KB per row

			//using (var importSession = new AdDataImportSession(this.Delivery))
			//{
			//    importSession.Begin();

			//    // ...............................................................
			//    // Read the ad report, and build a lookup table for later

			//    // create the ad report reader
			//    string[] requiredHeaders = new string[1];
			//    requiredHeaders[0] = WS.AdPerformanceReportColumn.AdId.ToString();
			//    var adReportReader = new CsvDynamicReader(adReport.OpenContents(), requiredHeaders);
					
				

			//    // How often (every how many items) to report progress
			//    const int reportEvery = 30;

			//    // read
			//    using (adReportReader)
			//    {
			//        int adcount = 0;

			//        while (adReportReader.Read())
			//        {
			//            // create the ad
			//            Ad ad = CreateAd(adReportReader.Current);
			//            //importSession.ImportAd(ad);
			//            _adCache.

			//            // Guess the progress
			//            progress += adSize / totalSize;
			//            adcount++;
			//            if (adcount % reportEvery == 0)
			//                ReportProgress(progress);
			//        }
			//        ReportProgress(progress);
			//    }

			//    // mark the delivery file as processed
			//    adReport.History.Add(DeliveryOperation.Processed, Instance.InstanceID);

			//    // ...............................................................
			//    // Read the keyword report, cross reference it with the ad data, and commit

			//    // The name of the time period column is specified by the initializer, depending on the report
			//    string timePeriodColumn = keywordReport.Parameters[Const.Parameters.TimePeriodColumnName] as string;

			//    // create the keyword report reader
			//    var keywordReportReader = new XmlDynamicReader
			//    (
			//        FileManager.Open(keywordReportFileInfo),
			//        Instance.Configuration.Options["AdCenter.KeywordPerformance.XPath"] // Report/Table/Row
			//    );

			//    // read and save in transaction
			//    using (keywordReportReader)
			//    {

			//        int kwcount = 0;
			//        while (keywordReportReader.Read())
			//        {
			//            // get the unit from the keyword report, and add the missing ad data
			//            AdMetricsUnit unit = CreateMetrics(keywordReportReader.Current, timePeriodColumn);
			//            importSession.ImportMetrics(unit);

			//            // Guess the progress
			//            progress += kwSize / totalSize;
			//            kwcount++;
			//            if (kwcount % reportEvery == 0)
			//                ReportProgress(progress);
			//        }

			//        ReportProgress(progress);
			//    }
			//}

			return ServiceOutcome.Success;
		}


		private Ad CreateAd(dynamic values)
		{
			Ad ad = new Ad()
			{
				OriginalID = values[WS.AdPerformanceReportColumn.AdId.ToString()],
				Campaign = new Campaign()
				{
					Account = new Account()
					{
						ID = Instance.AccountID,
						OriginalID = values[WS.AdPerformanceReportColumn.AccountNumber.ToString()]
					},
					Channel = this.Delivery.Channel
				},
				Creatives = new List<Creative>()
				{
					new TextCreative(){ TextType = TextCreativeType.Title, Text = values[WS.AdPerformanceReportColumn.AdTitle.ToString()] },
					new TextCreative(){ TextType = TextCreativeType.Body, Text = values[WS.AdPerformanceReportColumn.AdDescription.ToString()] }
				}
			};
			return ad;
		}

		private AdMetricsUnit CreateMetrics(dynamic values, string timePeriodColumn)
		{
			//if (String.IsNullOrWhiteSpace(timePeriodColumn))
			//    throw new ArgumentNullException("timePeriodColumn");

			var unit = new AdMetricsUnit();

			////..................
			//// TIME

			//// TODO: figure out in what format this is returned; doesn't look promising...
			//// (http://social.msdn.microsoft.com/Forums/en-US/adcenterdev/thread/155dba04-f541-489b-878c-d259f4a2814b)
			//string rawTime = values[timePeriodColumn];
			//unit.PeriodStart = unit.PeriodEnd = DateTime.ParseExact(rawTime, "d/M/yyyy", null); // wild guess


			////..................
			//// IDENTITIES

			////TODO: get ad from _adCache
			////unit.Ad = new Ad() { OriginalID = values[WS.KeywordPerformanceReportColumn.AdId.ToString()] };

			//// TODO: Tracker
			
			//// Targeting
			//string rawMatchType = values[WS.KeywordPerformanceReportColumn.MatchType.ToString()];
			//KeywordMatchType matchType = KeywordMatchType.Unidentified;
			//if (rawMatchType != null)
			//    Enum.TryParse<KeywordMatchType>(rawMatchType, out matchType);

			//unit.TargetMatches.Add(new KeywordTarget()
			//{
			//    MatchType		= matchType,
			//    OriginalID		= values[WS.KeywordPerformanceReportColumn.KeywordId.ToString()],
			//    Keyword			= values[WS.KeywordPerformanceReportColumn.Keyword.ToString()],
			//    DestinationUrl	= values[WS.KeywordPerformanceReportColumn.DestinationUrl.ToString()]
			//});

			//// Currency
			//unit.Currency = new Currency() { Code = values[WS.KeywordPerformanceReportColumn.CurrencyCode.ToString()] };

			////..................
			//// MEASURES

			//unit.Impressions		= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Impressions.ToString()]);
			//unit.Clicks				= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Clicks.ToString()]);
			//unit.Cost				= Double.Parse(values[WS.KeywordPerformanceReportColumn.Spend.ToString()]);
			//unit.AveragePosition	= Double.Parse(values[WS.KeywordPerformanceReportColumn.AveragePosition.ToString()]);
			////input.Conversions		= Int32.Parse(values[WS.KeywordPerformanceReportColumn.Conversions		.ToString()]);

			return unit;
		}
	}

}
