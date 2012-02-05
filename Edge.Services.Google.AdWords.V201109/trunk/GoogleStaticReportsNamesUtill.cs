using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GA =Google.Api.Ads.AdWords.v201109;

namespace Edge.Services.Google.AdWords
{
	public static class GoogleStaticReportsNamesUtill
	{
		public static Dictionary<GA.ReportDefinitionReportType, string> _reportNames = new Dictionary<GA.ReportDefinitionReportType, string>()
		{
			{GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT, "KEYWORDS_PERF"},
			{GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, "AD_PERF"},
			{GA.ReportDefinitionReportType.URL_PERFORMANCE_REPORT, "URL_PERF"},
			{GA.ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT, "ADGROUP_PERF"},
			{GA.ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT, "CAMPAIGN_PERF"},
			{GA.ReportDefinitionReportType.ACCOUNT_PERFORMANCE_REPORT, "ACCOUNT_PERF"},
			{GA.ReportDefinitionReportType.DEMOGRAPHIC_PERFORMANCE_REPORT, "DEMOGRAPHIC_PERF"},
			{GA.ReportDefinitionReportType.GEO_PERFORMANCE_REPORT, "GEO_PERF"},
			{GA.ReportDefinitionReportType.SEARCH_QUERY_PERFORMANCE_REPORT, "SEARCH_QUERY_PERF"},
			{GA.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT, "MANAGED_PLAC_PERF"},
			{GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT, "AUTOMATIC_PLAC_PERF"},
			{GA.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_KEYWORDS_PERF"},
			{GA.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_PLACEMENTS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_PLACEMENTS_PERF"},
			{GA.ReportDefinitionReportType.AD_EXTENSIONS_PERFORMANCE_REPORT, "AD_EXTENSIONS_PERF"},
			{GA.ReportDefinitionReportType.DESTINATION_URL_REPORT, "DEST_URL_REP"},
			{GA.ReportDefinitionReportType.CREATIVE_CONVERSION_REPORT, "CREATIVE_CONV_REP"},
			{GA.ReportDefinitionReportType.UNKNOWN, ""}
		};

	}  
}
