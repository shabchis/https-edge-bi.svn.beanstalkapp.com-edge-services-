using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using Google.Api.Ads.AdWords.v201109;
using Google.Api.Ads.AdWords.Util;
using System.Threading;
using Edge.Core.Utilities;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util.Reports;

namespace Edge.Services.Google.AdWords
{
	class RetrieverService : PipelineService
	{

		#region members
		private BatchDownloadOperation _batchDownloadOperation;
		private int _filesInProgress = 0;
		private double _minProgress = 0.05;
		AdwordsReport _googleReport;
		ReportDefinitionDateRangeType _dateRange;
		private AutoResetEvent _waitHandle;
		long _reportId;

		#endregion

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			_batchDownloadOperation = new BatchDownloadOperation();
			_batchDownloadOperation.Progressed += new EventHandler(_batchDownloadOperation_Progressed);
			_filesInProgress = this.Delivery.Files.Count;
			bool includeZeroImpression = Boolean.Parse(this.Delivery.Parameters["includeZeroImpression"].ToString());


			//Sets Date Range and time period
			_dateRange = ReportDefinitionDateRangeType.CUSTOM_DATE;
			string startDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
			string endDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");
			_waitHandle = new AutoResetEvent(false);

			Dictionary<string, string> headers = new Dictionary<string, string>()
			{
				{"DeveloperToken" , "5eCsvAOU06Fs4j5qHWKTCA"},
				{"UserAgent" , "Edge.BI"},
				{"EnableGzipCompression","true"},
				{"ClientCustomerId","281-492-7878"},
				{"Email","ppc.easynet@gmail.com"},
				{"Password","mccpass2012"}
			};

			AdWordsUser user = new AdWordsUser(headers);

			//Create ReportDefintion
			ReportDefinition definition = new ReportDefinition();

			definition.reportName = "Last 7 days ADGROUP_PERFORMANCE_REPORT";
			definition.reportType = ReportDefinitionReportType.AD_PERFORMANCE_REPORT;
			definition.downloadFormat = DownloadFormat.GZIPPED_CSV;
			definition.dateRangeType = ReportDefinitionDateRangeType.YESTERDAY;

			// Get the ReportDefinitionService.
			ReportDefinitionService reportDefinitionService = (ReportDefinitionService)user.GetService(
			AdWordsService.v201109.ReportDefinitionService);

			// Create the selector.
			Selector selector = new Selector();
			selector.fields = new string[] { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions","Clicks", "Cost","Headline",
		                                                   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl","CampaignStatus","AccountTimeZoneId",
		                                                   "AdType","AccountCurrencyCode","Ctr","Status","AveragePosition","Conversions",
		                                                   "ConversionRate","ConversionRateManyPerClick","ConversionSignificance",
		                                                   "ConversionsManyPerClick",
		                                                   "ConversionValue","TotalConvValue","ValuePerConversion","ValuePerConversionManyPerClick","ValuePerConvManyPerClick","ViewThroughConversions","ViewThroughConversionsSignificance",
		                                                   "AdNetworkType1"
		                                               };

			definition.selector = selector;

			foreach (string clientId in (string[])this.Delivery.Parameters["AdwordsClientIDs"])
			{
				//Get all files on specific client
				var files = from f in this.Delivery.Files
							where f.Parameters["AdwordsClientID"].ToString() == clientId
							select f;

				foreach (var file in files)
				{
					

					

					//_googleReport.CreateGoogleReport();


					new ReportUtilities(user).DownloadClientReport(definition, @"D:\gaTest.gzip");



					InitalizeReportParams(file);
					DownloadFile(file);

					//try
					//{
					//    _googleReport.intializingGoogleReport();
					//}
					//catch (AdWordsApiException e)
					//{
					//    bool InvalidreportID = false;
					//    bool retry = true;
					//    _googleReport.intializingGoogleReport(InvalidreportID, retry);
					//}

					//InitalizeReportParams(file);

					//try
					//{
					//    DownloadFile(file, this._googleReport);
					//}
					//catch (FileDownloadException ex) // if Getting a report ID exception
					//{

					//    Log.Write(ex.Message, LogMessageType.Warning);
					//    bool invalidReportID = true;
					//    _googleReport.intializingGoogleReport(invalidReportID); // Report ID is invalid - create new report ID
					//    Log.Write("Retriever : renewing Google Auth key", ex);
					//    InitalizeReportParams(file);
					//    DownloadFile(file);


					//}
				}

			}
			_batchDownloadOperation.Start();
			_batchDownloadOperation.Wait();
			_batchDownloadOperation.EnsureSuccess();
			this.Delivery.Save();

			return Core.Services.ServiceOutcome.Success;

		}

		private void InitalizeReportParams(DeliveryFile file)
		{
			GoogleRequestEntity request = _googleReport.GetReportUrlParams(true);

			file.SourceUrl = request.DownloadUrl;
			file.Parameters.Add("clientEmail", request.ClientEmail);
			file.Parameters.Add("clientCustomerId", request.ClientCustomerId);
			file.Parameters.Add("authToken", request.AuthToken);
			file.Parameters.Add("returnMoneyInMicros", request.ReturnMoneyInMicros);
			file.Parameters.Add("developerToken", request.DeveloperToken);
			file.Parameters.Add("RequestBody", request.Body);
			file.Parameters.Add("EnableGzipCompression", request.EnableGzipCompression);


		}

		void _batchDownloadOperation_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(DownloadOperation.Progress);
		}


		private void DownloadFile(DeliveryFile file)
		{

			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);

			request.Headers.Add("clientEmail: " + file.Parameters["clientEmail"]);
			request.Headers.Add("clientCustomerId: " + file.Parameters["clientCustomerId"]);
			request.Headers.Add("Authorization: GoogleLogin auth=" + file.Parameters["authToken"]);
			request.Headers.Add("returnMoneyInMicros: " + file.Parameters["returnMoneyInMicros"]);
			request.Headers.Add("developerToken: " + file.Parameters["developerToken"]);
			request.Method = "POST";

			if (Convert.ToBoolean(file.Parameters["EnableGzipCompression"]))
			{
				request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
			}
			else request.AutomaticDecompression = DecompressionMethods.None;

			byte[] bytes = Encoding.UTF8.GetBytes(file.Parameters["RequestBody"].ToString());
			request.ContentLength = bytes.Length;

			using (var stream = request.GetRequestStream())
			{
				stream.Write(bytes, 0, bytes.Length);
			}

			_batchDownloadOperation.Add(file.Download(request));


		}
	}
}
