using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GA =Google.Api.Ads.AdWords.v201101;

namespace Edge.Services.Google.Adwords
{
	public static class GoogleStaticReportsNamesUtill
	{
		public static Dictionary<GA.ReportDefinitionReportType, string> _reportNames;

		public GoogleStaticReportsNamesUtill()
		{
			_reportNames = new Dictionary<GA.ReportDefinitionReportType, string>();
			_reportNames.Add(GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT, "KEYWORDS_PERF");//0
			_reportNames.Add(GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, "AD_PERF");//1
			_reportNames.Add(GA.ReportDefinitionReportType.URL_PERFORMANCE_REPORT, "URL_PERF");//2
			_reportNames.Add(GA.ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT, "ADGROUP_PERF");//3
			_reportNames.Add(GA.ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT, "CAMPAIGN_PERF");//4
			_reportNames.Add(GA.ReportDefinitionReportType.ACCOUNT_PERFORMANCE_REPORT, "ACCOUNT_PERF");//5
			_reportNames.Add(GA.ReportDefinitionReportType.DEMOGRAPHIC_PERFORMANCE_REPORT, "DEMOGRAPHIC_PERF");//6
			_reportNames.Add(GA.ReportDefinitionReportType.GEO_PERFORMANCE_REPORT, "GEO_PERF");//7
			_reportNames.Add(GA.ReportDefinitionReportType.SEARCH_QUERY_PERFORMANCE_REPORT, "SEARCH_QUERY_PERF");//8
			_reportNames.Add(GA.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT, "MANAGED_PLAC_PERF");//9
			_reportNames.Add(GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT, "AUTOMATIC_PLAC_PERF");//10
			_reportNames.Add(GA.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_KEYWORDS_PERF");//11
			_reportNames.Add(GA.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_PLACEMENTS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_PLACEMENTS_PERF");//12
			_reportNames.Add(GA.ReportDefinitionReportType.AD_EXTENSIONS_PERFORMANCE_REPORT, "AD_EXTENSIONS_PERF");//13
			_reportNames.Add(GA.ReportDefinitionReportType.DESTINATION_URL_REPORT, "DEST_URL_REP");//14
			_reportNames.Add(GA.ReportDefinitionReportType.CREATIVE_CONVERSION_REPORT, "CREATIVE_CONV_REP");//15
			_reportNames.Add(GA.ReportDefinitionReportType.UNKNOWN, "");//16
		}
	}  
}
