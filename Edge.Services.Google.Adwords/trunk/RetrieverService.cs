using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using Google.Api.Ads.AdWords.v201101;
using Google.Api.Ads.AdWords.Util;
using System.Threading;
using Edge.Core.Utilities;
using Google.Api.Ads.AdWords.Lib;

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
			bool includeConversionTypes = Boolean.Parse(this.Delivery.Parameters["includeConversionTypes"].ToString());
			bool includeDisplaytData = Boolean.Parse(this.Delivery.Parameters["includeDisplaytData"].ToString());

			//Sets Date Range and time period
			_dateRange = ReportDefinitionDateRangeType.CUSTOM_DATE;
			string startDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
			string endDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");
			_waitHandle = new AutoResetEvent(false);

			foreach (string email in (string[])this.Delivery.Parameters["accountEmails"])
			{
				//Get all files on specific email
				var files = from f in this.Delivery.Files
							where f.Parameters["Email"].ToString() == email
							select f;

				foreach (var file in files)
				{
					if (file.Name.ToString().Equals(GoogleStaticReportsNamesUtill._reportNames[ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv"))
					{
						_googleReport = new AdwordsReport(Instance.AccountID, this.Delivery.Parameters["MccEmail"].ToString(), email, startDate, endDate, false, _dateRange,
														ReportDefinitionReportType.AD_PERFORMANCE_REPORT, true);
					}
					else if (file.Name.ToString().Equals(GoogleStaticReportsNamesUtill._reportNames[ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT]))
					{
						_googleReport = new AdwordsReport(Instance.AccountID, this.Delivery.Parameters["MccEmail"].ToString(), email, startDate, endDate, false, _dateRange,
														ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT);
					}
					//else if (file.Name.ToString().Equals(GoogleStaticReportsNamesUtill._reportNames[ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT]))
					//{
					//    _googleReport = new AdwordsReport(Instance.AccountID, this.Delivery.Parameters["MccEmail"].ToString(), email, startDate, endDate, false, _dateRange,
					//                                    ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT);
					//}
					else
					{//Other
						var report = from r in GoogleStaticReportsNamesUtill._reportNames
									 where r.Value == file.Name
									 select r.Key;

						_googleReport = new AdwordsReport(Instance.AccountID, this.Delivery.Parameters["MccEmail"].ToString(), email, startDate, endDate, includeZeroImpression, _dateRange, (ReportDefinitionReportType)report.First());
					}
					try
					{
						_googleReport.intializingGoogleReport();
					}
					catch (AdWordsApiException e)
					{
						bool InvalidreportID = false;
						bool retry = true;
						_googleReport.intializingGoogleReport(InvalidreportID, retry);
					}

					InitalizeReportParams(file);

					try
					{
						DownloadFile(file);
					}
					catch (FileDownloadException ex) // if Getting a report ID exception
					{

						Log.Write(ex.Message, LogMessageType.Warning);
						bool invalidReportID = true;
						_googleReport.intializingGoogleReport(invalidReportID); // Report ID is invalid - create new report ID
						Log.Write("Retriever : renewing Google Auth key", ex);
						InitalizeReportParams(file);
						DownloadFile(file);


					}
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

			file.Name = _googleReport.customizedReportName + ".gz";
			file.SourceUrl = request.downloadUrl.ToString();
			file.Parameters.Add("clientCustomerId", request.clientCustomerId);
			file.Parameters.Add("authToken", request.authToken);
			file.Parameters.Add("returnMoneyInMicros", request.returnMoneyInMicros);
		}

		void _batchDownloadOperation_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(DownloadOperation.Progress);
		}


		private void DownloadFile(DeliveryFile file)
		{

			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);

			request.Headers.Add("clientCustomerId: " + file.Parameters["clientCustomerId"]);
			request.Headers.Add("clientEmail: " + file.Parameters["Email"]);
			request.Headers.Add("Authorization: GoogleLogin auth=" + file.Parameters["authToken"]);
			request.Headers.Add("returnMoneyInMicros: " + file.Parameters["returnMoneyInMicros"]);
			//request.Method = "POST";
			_batchDownloadOperation.Add(file.Download(request));

		}
	}
}
