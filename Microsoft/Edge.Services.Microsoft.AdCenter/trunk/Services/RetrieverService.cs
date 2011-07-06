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
		BatchDownloadOperation _batchDownload;
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			_adCenterApi = new AdCenterApi(this);
			_filesInProgress = this.Delivery.Files.Count;
			bool result = true;


			CreateRequests();
			if (!Download())
			{
				CreateRequests();
				result=Download();
			}
			if (result == false)
			{
				Log.Write("Retriver error see last warnings!!", LogMessageType.Error);
				return Core.Services.ServiceOutcome.Failure;
			}


			

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
				WS.AdPerformanceReportColumn.AccountNumber, // not necessary for this service version
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
					string innerFileName;
					adReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(adReportRequest, out innerFileName);
					adReportFile.Parameters["InnerFileName"] = string.Format(@"\{0}.Csv", innerFileName);
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
					string innerFileName;
					campaignReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(campaignReportRequest,out innerFileName);
					campaignReportFile.Parameters["InnerFileName"]= string.Format(@"\{0}.Csv", innerFileName);

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
					string innerFileName;
					keywordReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(keywordReportRequest,out innerFileName);
					keywordReportFile.Parameters["InnerFileName"]= string.Format(@"\{0}.Csv", innerFileName);
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

		private bool Download()
		{
			 _batchDownload = new BatchDownloadOperation();
			
			bool result = true;
			foreach (DeliveryFile file in this.Delivery.Files)
			{
				try
				{
					_batchDownload.Add(file.Download());
					
				}
				catch (WebException webEx)
				{
					Log.Write("web alert", webEx.InnerException, LogMessageType.Warning);
					result = false;
					foreach (DeliveryFile deliveryFile in this.Delivery.Files)
					{
						deliveryFile.SourceUrl = string.Empty;
						
					}
					break;

				}

				
				
			}
			_batchDownload.Progressed += new EventHandler(_batchDownload_Progressed);
			_batchDownload.Start();
			_batchDownload.Wait();
			try
			{
				_batchDownload.EnsureSuccess();
			}
			catch (Exception ex)
			{
				Log.Write("DownloadError",ex, LogMessageType.Warning);
				result = false;
			}
			
			return result;
			
		}

		void _batchDownload_Progressed(object sender, EventArgs e)
		{
			throw new NotImplementedException();
		}

		

		
	}
}
