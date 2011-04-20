using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Google.Adwords
{
    class AdwordsReport 
    {
        public AdwordsReport()
        {
            IsZipped = false;
            Format = ReportFormat.XML;
            AdWordsType = AdwordsReportService.AdWordsType.SearchOnly;
            includeZeroImpression = false;
        }

        public AdwordsReportService.AdWordsType AdWordsType { set; get; }
        public ReportType ReportType { set; get; }
        public Dictionary<string, string> FieldsMapping { set; get; }
        public DateTime StartDate { set; get; } // get from configuration
        public DateTime EndDate { set; get; }// get from configuration
        public bool IsZipped { get; set; }
        public ReportFormat Format { get; set; }
        public bool includeZeroImpression { get; set; }
        public string[] selectedColumns { set; get; } // Get Selected Columns from configuration 

    }

    enum ReportFormat
    {
        CSV,
        XML
    };

    enum ReportType
    {
        Account,
        AdGroup,
        Campaign,
        Demographic,
        ContentPlacement,
        Creative,
        Geographic,
        Keyword,
        Query,
        ReachAndFrequency,
        Structure,
        Url
    }

}
