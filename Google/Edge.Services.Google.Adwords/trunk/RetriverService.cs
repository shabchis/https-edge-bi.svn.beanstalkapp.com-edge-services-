using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using Google.Api.Ads.AdWords.v201101;

namespace Edge.Services.Google.Adwords
{
	class RetriverService : PipelineService
	{
		#region members
		private int _countedFile = 0;
		private double _minProgress = 0.05;
		AccountEntity _edgeAccount;
		AdwordsReport _googleReport;
		List<ReportDefinitionReportType> _reportsTypes = new List<ReportDefinitionReportType>();
		ReportDefinitionDateRangeType _dateRange;
		long _reportId;

		#endregion
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			_googleReport = new AdwordsReport();
			_edgeAccount = new AccountEntity(Instance.AccountID, this.Delivery.Parameters["AdwordsEmail"].ToString());

			//reportTypes
			foreach (string type in this.Delivery.Parameters["ReportsType"].ToString().Split('|').ToList<string>())
			{
				if (Enum.IsDefined(typeof(ReportDefinitionReportType), type))
					_reportsTypes.Add((ReportDefinitionReportType)Enum.Parse(typeof(ReportDefinitionReportType), type, true));
				else throw new Exception("Undefined ReportType");
			}

			//Date Range
			//Temp
			_dateRange = ReportDefinitionDateRangeType.CUSTOM_DATE;
			
			try
			{
				this._googleReport.StartDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
				this._googleReport.EndDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");
			}
			catch (Exception e)
			{
				throw new Exception("Cannot set start/end time from TargetPeriod", e);
			}


			foreach (string email in _edgeAccount.Emails)
			{
				List<DeliveryFile> filesPerEmail = new List<DeliveryFile>();
				foreach (ReportDefinitionReportType type in _reportsTypes)
				{
					_googleReport.SetReportDefinition(email, _dateRange, type);
					_reportId = _googleReport.intializingGoogleReport(Instance.AccountID, Instance.InstanceID);
					
					//FOR DEBUG
					//_googleReport.DownloadReport(_reportId);

					GoogleRequestEntity request = _googleReport.GetReportUrlParams(true);

					DeliveryFile file = new DeliveryFile();
					file.Name = _googleReport.Name;
					file.SourceUrl = request.downloadUrl.ToString();
					file.Parameters.Add("GoogleRequestEntity", request);

					filesPerEmail.Add(file);
					this.Delivery.Files.Add(file);

				}
				Delivery.Parameters.Add(email, filesPerEmail);
			}


			_countedFile = this.Delivery.Files.Count;
			
			foreach ( DeliveryFile file in this.Delivery.Files)
			{
				DownloadFile(file);
				
			}
			return Core.Services.ServiceOutcome.Success;
		}

		private void DownloadFile(DeliveryFile file)
		{
					
			//string body = file.Parameters["body"].ToString();
			GoogleRequestEntity googleRequestEntity = (GoogleRequestEntity)file.Parameters["GoogleRequestEntity"];
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(googleRequestEntity.downloadUrl);
			
				request.Headers.Add("clientCustomerId: " + googleRequestEntity.clientCustomerId);

				request.Headers.Add("clientEmail: " + googleRequestEntity.clientEmail);

			
				request.Headers.Add("Authorization: GoogleLogin auth=" + googleRequestEntity.authToken);


				request.Headers.Add("returnMoneyInMicros: " + googleRequestEntity.returnMoneyInMicros);

			WebResponse response = request.GetResponse();

			FileDownloadOperation fileDownloadOperation = file.Download(response.GetResponseStream(), true, response.ContentLength); 
						
			fileDownloadOperation.Progressed += new EventHandler<ProgressEventArgs>(fileDownloadOperation_Progressed);
			fileDownloadOperation.Ended += new EventHandler<EndedEventArgs>(fileDownloadOperation_Ended);
			fileDownloadOperation.Start();
		}

		void fileDownloadOperation_Ended(object sender, EndedEventArgs e)
		{
			_countedFile -= 1;
		}

		void fileDownloadOperation_Progressed(object sender, ProgressEventArgs e)
		{
			if (_countedFile>0)
			{
				double percent = Math.Round(Convert.ToDouble(Convert.ToDouble(e.DownloadedBytes) / Convert.ToDouble(e.TotalBytes) / (double)_countedFile), 3);
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
