using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceModel;
using System.Text;
using Edge.Core;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Configuration;
using Edge.Data.Pipeline.Deliveries;
using Edge.Data.Pipeline.Readers;
using Edge.Data.Pipeline.Services;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;

namespace Edge.Services.Microsoft.AdCenter
{
	public class InitializerService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			// Create a new delivery
			this.Delivery = new Delivery(this.Instance.InstanceID)
			{
				TargetPeriod = this.TargetPeriod
			};

			// AccountID as parameter for entire delivery
			this.Delivery.Parameters[Const.Parameters.AccountID] = this.Instance.AccountID;

			// Wrapper for adCenter API
			AdCenterApi adCenterApi = new AdCenterApi(this);

			// Both keyword and ad performance reports are needed
			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = Const.Files.KeywordReport,
				SourceUrl = adCenterApi.SubmitReportRequest(adCenterApi.NewKeywordPerformanceReportRequest(
					WS.KeywordPerformanceReportColumn.TimePeriod, // special column
					WS.KeywordPerformanceReportColumn.AccountNumber,
					WS.KeywordPerformanceReportColumn.CampaignName,
					WS.KeywordPerformanceReportColumn.CampaignId,
					WS.KeywordPerformanceReportColumn.AdGroupName,
					WS.KeywordPerformanceReportColumn.AdGroupId,
					WS.KeywordPerformanceReportColumn.Keyword,
					WS.KeywordPerformanceReportColumn.KeywordId,
					WS.KeywordPerformanceReportColumn.AdId,
					WS.KeywordPerformanceReportColumn.DestinationUrl,
					WS.KeywordPerformanceReportColumn.MatchType,
					WS.KeywordPerformanceReportColumn.CurrencyCode,
					WS.KeywordPerformanceReportColumn.Impressions,
					WS.KeywordPerformanceReportColumn.Clicks,
					WS.KeywordPerformanceReportColumn.Spend,
					WS.KeywordPerformanceReportColumn.AveragePosition,
					WS.KeywordPerformanceReportColumn.Conversions
				)),
				Parameters = new Dictionary<string, object>()
				{
					{Const.Parameters.TimePeriodColumnName, AdCenterApi.GetTimePeriodColumnName(WS.ReportAggregation.Daily)}
				}
			});
			ReportProgress(0.49); // progress: 49%

			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = Const.Files.AdReport,
				SourceUrl = adCenterApi.SubmitReportRequest(adCenterApi.NewAdPerformanceReportRequest(
					WS.AdPerformanceReportColumn.AdId,
					WS.AdPerformanceReportColumn.AdTitle,
					WS.AdPerformanceReportColumn.AdDescription
				))
			});
			ReportProgress(0.98); // progress: 98%

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

	}
}
