using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Google.Api.Ads.AdWords.v201101;
using Google.Api.Ads.Common.Lib;

namespace Edge.Services.Google.Adwords
{
    public partial class Test : Form
    {
        public Test()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            // Create selector.
            Selector selector = new Selector();
            selector.fields = new String[] { "AdGroupId", "Id", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost" };
            // selector.setPredicates(new Predicate[] { adGroupPredicate });
            selector.dateRange.min = "20110401";
            selector.dateRange.max = "20110402";


            // Create ClientSelector.
            List<ClientSelector> clients = new List<ClientSelector>();
            ClientSelector client = new ClientSelector();
            client.login = "client@gmail.com";
            clients.Add(client);

            // Create report definition.
            ReportDefinition _reportDefinition = new ReportDefinition();
            _reportDefinition.reportName = "Adword Creative Report - TestDaily";
            _reportDefinition.reportType = ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT;
            _reportDefinition.downloadFormat = DownloadFormat.GZIPPED_CSV;
            _reportDefinition.downloadFormatSpecified = true;
            _reportDefinition.clientSelectors = clients.ToArray();

            // Create operations.
            ReportDefinitionOperation operation = new ReportDefinitionOperation();
            operation.operand = _reportDefinition;
            operation.@operator = Operator.ADD;
            ReportDefinitionOperation[] operations = new ReportDefinitionOperation[] { operation };

            // Create ReportDefinitionService.
            ReportDefinitionService _reportDefinitionService = new ReportDefinitionService();
            _reportDefinitionService.RequestHeader.developerToken = "5eCsvAOU06Fs4j5qHWKTCA";
            _reportDefinitionService.RequestHeader.clientEmail = "edge.bi.mcc@gmail.com";
            //_reportDefinitionService.RequestHeader. = "edge.bi.mcc@gmail.com";

            _reportDefinitionService.RequestHeader.validateOnly = true;


            // Add report definition.
            ReportDefinition[] result = _reportDefinitionService.mutate(operations); 
            
          

           

        }
    }
}
