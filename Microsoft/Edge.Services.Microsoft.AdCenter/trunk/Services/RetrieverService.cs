using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Threading;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;
using System.Net;
using Edge.Core.Utilities;
namespace Edge.Services.Microsoft.AdCenter
{
	class RetrieverService : PipelineService
	{
		private AutoResetEvent _waitHandle;
		private int _filesInProgress = 0;
		private double _minProgress = 0.05;
		AdCenterApi _adCenterApi;
		DeliveryFile _adReportFile;
			DeliveryFile _campaignReportFile;
			DeliveryFile _keyWordReportFile;
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			 _adCenterApi=new AdCenterApi(this);
			 _filesInProgress = this.Delivery.Files.Count;
			 DeliveryFile adReportFile;
			 DeliveryFile campaignReportFile;


			 CreateRequests();
			

			
			//Download(adReportFile,adReportRequest);
			//Download(keywordReportFile,keywordReportRequest);
			//Download(campaignReportFile,campaignReportRequest);


			_waitHandle.WaitOne();

			//Download(adReportFile, reportRequest);

			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;
		}



		private void CreateRequests()
		{
			List<ManualResetEvent> manualEvents = new List<ManualResetEvent>();


			_waitHandle = new AutoResetEvent(false);


			//ADREPORT----------------------------------------
			DeliveryFile adReportFile = this.Delivery.Files[Const.Files.AdReport];
			WS.ReportRequest adReportRequest;
			adReportRequest = _adCenterApi.NewAdPerformanceReportRequest
			 (
				WS.AdPerformanceReportColumn.AccountName, //required field
				//WS.AdPerformanceReportColumn.AccountNumber, // not necessary for this service version
				WS.AdPerformanceReportColumn.CampaignName,//required field
				WS.AdPerformanceReportColumn.TimePeriod,//required field if aggregation time!=Summary
				//WS.AdPerformanceReportColumn.CampaignId, // why did MS leave this out? stupid fucks
				WS.AdPerformanceReportColumn.AdGroupName,
				WS.AdPerformanceReportColumn.AdGroupId,
				WS.AdPerformanceReportColumn.AdId,
				WS.AdPerformanceReportColumn.AdTitle,
				WS.AdPerformanceReportColumn.AdDescription,
				WS.AdPerformanceReportColumn.DestinationUrl,
				WS.AdPerformanceReportColumn.Clicks
			);

			if (string.IsNullOrEmpty(adReportFile.SourceUrl))
			{
				ManualResetEvent asyncWait = new ManualResetEvent(false);
				manualEvents.Add(asyncWait);
				Action getAdReportUrl = () =>
				{
					adReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(adReportRequest);
				};

				getAdReportUrl.BeginInvoke(result =>
				{
					asyncWait.Set();
				},
				null);
			}


			//CAMPAIGNREPORT----------------------------------------
			DeliveryFile campaignReportFile = this.Delivery.Files[Const.Files.CampaignReport];
			WS.ReportRequest campaignReportRequest;
			campaignReportRequest = _adCenterApi.NewCampaignPerformanceReportRequest
				(
					WS.CampaignPerformanceReportColumn.AccountName,
					WS.CampaignPerformanceReportColumn.CampaignName,
					WS.CampaignPerformanceReportColumn.TimePeriod,
					WS.CampaignPerformanceReportColumn.CampaignId,
					WS.CampaignPerformanceReportColumn.Status,
					WS.CampaignPerformanceReportColumn.Clicks
				);

			if (string.IsNullOrEmpty(campaignReportFile.SourceUrl))
			{
				ManualResetEvent asyncWait = new ManualResetEvent(false);
				manualEvents.Add(asyncWait);
				Action getCampaignReportUrl = () =>
				{
					campaignReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(campaignReportRequest);
				};

				getCampaignReportUrl.BeginInvoke(result =>
				{
					asyncWait.Set();
				},
				null);

			}
			//KEYWORDREPORT----------------------------------------
			DeliveryFile keywordReportFile = this.Delivery.Files[Const.Files.KeywordReport];
			WS.ReportRequest keywordReportRequest;
			keywordReportRequest = _adCenterApi.NewKeywordPerformanceReportRequest(
					WS.KeywordPerformanceReportColumn.TimePeriod, // special column
					WS.KeywordPerformanceReportColumn.AdId,
					WS.KeywordPerformanceReportColumn.Keyword,
					WS.KeywordPerformanceReportColumn.KeywordId,
					WS.KeywordPerformanceReportColumn.DestinationUrl,
					WS.KeywordPerformanceReportColumn.MatchType,
					WS.KeywordPerformanceReportColumn.CurrencyCode,
					WS.KeywordPerformanceReportColumn.Impressions,
					WS.KeywordPerformanceReportColumn.Clicks,
					WS.KeywordPerformanceReportColumn.Spend,
					WS.KeywordPerformanceReportColumn.AveragePosition,
					WS.KeywordPerformanceReportColumn.Conversions
				);

			if (string.IsNullOrEmpty(keywordReportFile.SourceUrl))
			{
				ManualResetEvent asyncWait = new ManualResetEvent(false);
				manualEvents.Add(asyncWait);
				Action getKeywordReportUrl = () =>
				{
					keywordReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(keywordReportRequest);
				};

				getKeywordReportUrl.BeginInvoke(result =>
				{
					asyncWait.Set();
				},
				null);
			}


			if (manualEvents.Count > 1)
				WaitHandle.WaitAll(manualEvents.ToArray());
		}

		private void Download(DeliveryFile file,WS.ReportRequest reportRequest)
		{
			DeliveryFileDownloadOperation operation;
			try
			{
				operation = file.Download();
			}
			catch (WebException webEx)
			{
				Log.Write("web alert", webEx.InnerException, LogMessageType.Warning);
				file.SourceUrl = _adCenterApi.SubmitReportRequest(reportRequest);
				try
				{
					operation = file.Download();

				}
				catch (WebException Exception)
				{

					throw new WebException("Web exception", Exception, Exception.Status, Exception.Response);
				}

			}

			operation.Progressed += new EventHandler<ProgressEventArgs>(operation_Progressed);
			operation.Ended += new EventHandler<EndedEventArgs>(operation_Ended);
			operation.Start();
		}

		void operation_Ended(object sender, EndedEventArgs e)
		{
			_filesInProgress -= 1;
			if (_filesInProgress==0)			
				_waitHandle.Set();
			
		}

		void operation_Progressed(object sender, ProgressEventArgs e)
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
