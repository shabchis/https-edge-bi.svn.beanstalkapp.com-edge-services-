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

namespace Edge.Services.Google.Adwords
{
	class RetrieverService : PipelineService
	{

		#region members
		private int _filesInProgress = 0;
		private double _minProgress = 0.05;
		AdwordsReport _googleReport;
		ReportDefinitionDateRangeType _dateRange;
		private AutoResetEvent _waitHandle;
		long _reportId;

		#endregion

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			_filesInProgress = this.Delivery.Files.Count;
			bool includeZeroImpression = Boolean.Parse(this.Delivery.Parameters["includeZeroImpression"].ToString());

			//Date Range
			//TODO : GET DATE RANGE FROM TARGET PERIOD PARAM
			_dateRange = ReportDefinitionDateRangeType.CUSTOM_DATE;

			string startDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
			string endDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");
			_waitHandle = new AutoResetEvent(false);
			foreach (string email in (string[])this.Delivery.Parameters["accountEmails"])
			{
				//TO DO : Get the files on a specific email
				var files = from f in this.Delivery.Files
							where f.Parameters["Email"].ToString() == email
							select f;//this.Delivery.Files[email];
				
				foreach (var file in files)
				{
					_googleReport = new AdwordsReport(Instance.AccountID, email, startDate, endDate, includeZeroImpression, _dateRange,
								(ReportDefinitionReportType)Enum.Parse(typeof(ReportDefinitionReportType), file.Name.ToString(), true));
					_googleReport.intializingGoogleReport();

					GoogleRequestEntity request = _googleReport.GetReportUrlParams(true);

					file.Name = _googleReport.Name + ".zip";
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
						}
					}


				}

			}
			_waitHandle.WaitOne();
			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;
		}


		private void DownloadFile(DeliveryFile file)
		{

			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);

			request.Headers.Add("clientCustomerId: " + file.Parameters["clientCustomerId"]);
			request.Headers.Add("clientEmail: " + file.Parameters["Email"]);
			request.Headers.Add("Authorization: GoogleLogin auth=" + file.Parameters["authToken"]);
			request.Headers.Add("returnMoneyInMicros: " + file.Parameters["returnMoneyInMicros"]);

			WebResponse response = request.GetResponse();

			FileDownloadOperation fileDownloadOperation = file.Download(response.GetResponseStream(), true, response.ContentLength);

			fileDownloadOperation.Progressed += new EventHandler<ProgressEventArgs>(fileDownloadOperation_Progressed);
			fileDownloadOperation.Ended += new EventHandler<EndedEventArgs>(fileDownloadOperation_Ended);
			fileDownloadOperation.Start();
		}

		void fileDownloadOperation_Ended(object sender, EndedEventArgs e)
		{
			_filesInProgress -= 1;
			if (_filesInProgress==0)
			_waitHandle.Set();
		}

		void fileDownloadOperation_Progressed(object sender, ProgressEventArgs e)
		{
			if (_filesInProgress > 0)
			{
				double percent = Math.Round(Convert.ToDouble(Convert.ToDouble(e.DownloadedBytes) / Convert.ToDouble(e.TotalBytes) / (double)_filesInProgress), 3);
				if (percent >= _minProgress)
				{
					_minProgress += 0.05;
					if (percent <= 1)
						this.ReportProgress(percent);
				}
			}
		}


	}
}
