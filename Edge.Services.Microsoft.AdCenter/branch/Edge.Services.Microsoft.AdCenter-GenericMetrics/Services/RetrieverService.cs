using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Threading;
using WS = Edge.Services.Microsoft.AdCenter.AdCenter.Reporting;
using System.Net;
using Edge.Core.Utilities;
namespace Edge.Services.Microsoft.AdCenter
{
	class RetrieverService : PipelineService
	{
        private BatchDownloadOperation _batchDownloadOperation;
		private AutoResetEvent _waitHandle;
		private int _filesInProgress = 0;
		private double _minProgress = 0.05;
		AdCenterApi _adCenterApi;
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
            _batchDownloadOperation = new BatchDownloadOperation();
            _batchDownloadOperation.Progressed += new EventHandler(_batchDownloadOperation_Progressed);
            _batchDownloadOperation.Ended += new EventHandler(_batchDownloadOperation_Ended);
            
            _adCenterApi = new AdCenterApi(this);
			_filesInProgress = this.Delivery.Files.Count;

			CreateRequests();
			if (!Download())
			{
				CreateRequests();
				Download();
			}
            _batchDownloadOperation.Start();
            _batchDownloadOperation.Wait();
            _batchDownloadOperation.EnsureSuccess();
			//_waitHandle.WaitOne();

			//Download(adReportFile, reportRequest);

          

			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;
		}

        void _batchDownloadOperation_Ended(object sender, EventArgs e)
        {
            
        }

        void _batchDownloadOperation_Progressed(object sender, EventArgs e)
        {
            BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
            this.ReportProgress(DownloadOperation.Progress);
        }

		private void CreateRequests()
		{
			 List<ManualResetEvent> manualEvents = new List<ManualResetEvent>();

			_waitHandle = new AutoResetEvent(false);

            #region ADREPORT
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
                WS.AdPerformanceReportColumn.Clicks,
                WS.AdPerformanceReportColumn.AdType
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
            #endregion

            #region CAMPAIGNREPORT
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
                    campaignReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(campaignReportRequest, out innerFileName);
                    campaignReportFile.Parameters["InnerFileName"] = string.Format(@"\{0}.Csv", innerFileName);

                };

                getCampaignReportUrl.BeginInvoke(result =>
                {
                    asyncWait.Set();
                },
                null);

            } 
            #endregion


            #region KEYWORDREPORT
            DeliveryFile keywordReportFile = this.Delivery.Files[Const.Files.KeywordReport];
            WS.ReportRequest keywordReportRequest;
            keywordReportRequest = _adCenterApi.NewKeywordPerformanceReportRequest(
                    WS.KeywordPerformanceReportColumn.TimePeriod, // special column
                    WS.KeywordPerformanceReportColumn.AdId,
                    WS.KeywordPerformanceReportColumn.AdGroupId,
                    WS.KeywordPerformanceReportColumn.CampaignId,
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
                    keywordReportFile.SourceUrl = _adCenterApi.SubmitReportRequest(keywordReportRequest, out innerFileName);
                    keywordReportFile.Parameters["InnerFileName"] = string.Format(@"\{0}.Csv", innerFileName);
                };

                getKeywordReportUrl.BeginInvoke(result =>
                {
                    asyncWait.Set();
                },
                null);
            } 
            #endregion

            if (manualEvents.Count > 1)
                WaitHandle.WaitAll(manualEvents.ToArray());
		}

		private bool Download()
		{
			bool result = true;
			foreach (DeliveryFile file in this.Delivery.Files)
			{
				try
				{
                    HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);
                    _batchDownloadOperation.Add(file.Download(request));
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

			return result;
		}

    }
}
