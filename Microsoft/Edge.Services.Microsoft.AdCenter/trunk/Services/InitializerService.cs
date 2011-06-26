using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceModel;
using System.Text;
using Edge.Core;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
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
				Account = new Account() { ID = this.Instance.AccountID },
				TargetPeriod = this.TargetPeriod
			};

			// Wrapper for adCenter API
			AdCenterApi adCenterApi = new AdCenterApi(this);

			// ................................
			// Campaign report
			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = Const.Files.CampaignReport,
				SourceUrl = adCenterApi.SubmitReportRequest(adCenterApi.NewCampaignPerformanceReportRequest(
					WS.CampaignPerformanceReportColumn.CampaignId,
					WS.CampaignPerformanceReportColumn.CampaignName,
					WS.CampaignPerformanceReportColumn.Status
				))
			});

			ReportProgress(0.33);

			// ................................
			// Ad report
			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = Const.Files.AdReport,
				SourceUrl = adCenterApi.SubmitReportRequest(adCenterApi.NewAdPerformanceReportRequest(
					WS.AdPerformanceReportColumn.AccountNumber,
					WS.AdPerformanceReportColumn.CampaignName,
					//WS.AdPerformanceReportColumn.CampaignId, // why did MS leave this out? stupid fucks
					WS.AdPerformanceReportColumn.AdGroupName,
					WS.AdPerformanceReportColumn.AdGroupId,
					WS.AdPerformanceReportColumn.AdId,
					WS.AdPerformanceReportColumn.AdTitle,
					WS.AdPerformanceReportColumn.AdDescription,
					WS.AdPerformanceReportColumn.DestinationUrl
				))
			});

			ReportProgress(0.33);

			// ................................
			// Keyword report
			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = Const.Files.KeywordReport,
				SourceUrl = adCenterApi.SubmitReportRequest(adCenterApi.NewKeywordPerformanceReportRequest(
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
				)),
				Parameters = new Dictionary<string, object>()
				{
					{Const.Parameters.TimePeriodColumnName, AdCenterApi.GetTimePeriodColumnName(WS.ReportAggregation.Daily)}
				}
			});
			
			ReportProgress(0.33);

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

	}
}
