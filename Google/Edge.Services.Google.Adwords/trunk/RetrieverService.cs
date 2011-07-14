﻿using System;
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

namespace Edge.Services.Google.Adwords
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

			//Setting Date Range and time period
			//TODO : GET DATE RANGE FROM TARGET PERIOD PARAM
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
					else
					{//Other
						var report=from r in GoogleStaticReportsNamesUtill._reportNames
							  where r.Value==file.Name
							  select r.Key;

						_googleReport = new AdwordsReport(Instance.AccountID, this.Delivery.Parameters["MccEmail"].ToString(), email, startDate, endDate, includeZeroImpression, _dateRange,(ReportDefinitionReportType)report.First());
					}

					_googleReport.intializingGoogleReport();
					GoogleRequestEntity request = _googleReport.GetReportUrlParams(true);

					file.Name = _googleReport.customizedReportName + ".zip";
					file.SourceUrl = request.downloadUrl.ToString();
					file.Parameters.Add("clientCustomerId", request.clientCustomerId);
					file.Parameters.Add("authToken", request.authToken);
					file.Parameters.Add("returnMoneyInMicros", request.returnMoneyInMicros);

					try
					{
						DownloadFile(file);
					}
					catch (ReportsException ex) // if Getting a report ID exception
					{
						if (ex.InnerException.Message.Equals("Report contents are invalid."))
						{
							_googleReport.intializingGoogleReport(true); // set new report
							Log.Write("Retriever : renewing Google Auth key", ex);
						}
					}
				}

			}
			_batchDownloadOperation.Start();
			_batchDownloadOperation.Wait();
			_batchDownloadOperation.EnsureSuccess();
			this.Delivery.Save();

			return Core.Services.ServiceOutcome.Success;

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

		//void fileDownloadOperation_Ended(object sender, EventArgs e)
		//{
		//    _filesInProgress -= 1;
		//    if (_filesInProgress == 0)
		//        _waitHandle.Set();
		//}

		//void fileDownloadOperation_Progressed(object sender, EventArgs e)
		//{
		//    if (_filesInProgress > 0)
		//    {
		//        double percent = Math.Round(Convert.ToDouble(Convert.ToDouble(e.DownloadedBytes) / Convert.ToDouble(e.TotalBytes) / (double)_filesInProgress), 3);
		//        if (percent >= _minProgress)
		//        {
		//            _minProgress += 0.05;
		//            if (percent <= 1)
		//                this.ReportProgress(percent);
		//        }
		//    }
		//}


	}
}
