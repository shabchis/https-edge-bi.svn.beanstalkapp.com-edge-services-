using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Api.Ads.AdWords.v201101;

namespace Edge.Services.Google.Adwords
{
    class AdwordsReport
    {

		static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions", "Clicks", "Cost", "CreativeDestinationUrl", "KeywordId", "Url" };
		static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost" };

        public AdwordsReport()
        {
			this.reportDefinition = new ReportDefinition();
			this.reportDefinition.reportType = ReportDefinitionReportType.AD_PERFORMANCE_REPORT;
        }

		public AdwordsReport(ReportDefinitionReportType ReportType)
		{
			this.reportDefinition = new ReportDefinition();
			this.reportDefinition.reportType = ReportType;
		}

		public void InitializeGoogleReport()
		{
			Selector selector = new Selector();
			switch (this.reportDefinition.reportType)
			{
				case ReportDefinitionReportType.AD_PERFORMANCE_REPORT:
					{
						selector.fields = AD_PERFORMANCE_REPORT_FIELDS;
						break;
					}
				case ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT:
					{
						selector.fields = KEYWORDS_PERFORMANCE_REPORT_FIELDS;
						break;
					}
			}
			// Create operations.
			ReportDefinitionOperation operation = new ReportDefinitionOperation();
			operation.operand = reportDefinition;
			operation.@operator = Operator.ADD;
			ReportDefinitionOperation[] operations = new ReportDefinitionOperation[] { operation };
			
				

		}

		public ReportDefinition CreateReportDefinition(string reportName, ReportDefinitionDateRangeType dateRageType,
			Selector selector, ClientSelector[] clients, DownloadFormat downloadFormat = DownloadFormat.GZIPPED_CSV)
		{
			reportDefinition.reportName = reportName;
			reportDefinition.dateRangeType = dateRageType;
			
			reportDefinition.selector = selector;
			reportDefinition.downloadFormat = downloadFormat;
			reportDefinition.downloadFormatSpecified = true;
			reportDefinition.clientSelectors = clients.ToArray();
			return reportDefinition;
		}

		private ReportDefinition reportDefinition { set; get; }
		private ReportDefinitionReportType ReportType { set; get; }
        public Dictionary<string, string> FieldsMapping { set; get; }
        public DateTime StartDate { set; get; } // get from configuration
        public DateTime EndDate { set; get; }// get from configuration
		public DownloadFormat downloadFormat { get; set; }
        public bool includeZeroImpression { get; set; }
        public string[] selectedColumns { set; get; } // Get Selected Columns from configuration 

    }
    
}
