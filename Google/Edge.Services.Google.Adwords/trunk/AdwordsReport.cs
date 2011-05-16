using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Api.Ads.AdWords.v201101;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util;

namespace Edge.Services.Google.Adwords
{
    class AdwordsReport
    {

		static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions", "Clicks", "Cost", "CreativeDestinationUrl", "KeywordId", "Url" };
		static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost" };

        public AdwordsReport()
        {
			
        }

		public AdwordsReport(List<String> accountEmails, ReportDefinitionReportType ReportType = ReportDefinitionReportType.AD_PERFORMANCE_REPORT)
		{
			this.reportDefinition = new ReportDefinition();
			this.reportDefinition.reportType = ReportType;
			SetAccountEmails(accountEmails);
			this.User = new GoogleUserEntity();
		}




		private void SetAccountEmails(List<string> accountEmails)
		{
			List<ClientSelector> Clients = new List<ClientSelector>();
			foreach (string email in accountEmails)
			{
				ClientSelector client = new ClientSelector();
				client.login = email;
				Clients.Add(client);
			}
			this.AccountEmails = Clients.ToArray();
		}



		public void InitializeGoogleReport(int Account_Id , long Instance_Id)
		{
			Selector selector = new Selector();
			switch (this.reportDefinition.reportType)
			{
				case ReportDefinitionReportType.AD_PERFORMANCE_REPORT:
					{
						selector.fields = AD_PERFORMANCE_REPORT_FIELDS;
						this.Name = "AD_PERFORMANCE_REPORT_"+Account_Id+"_"+Instance_Id;
						break;
					}
				case ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT:
					{
						selector.fields = KEYWORDS_PERFORMANCE_REPORT_FIELDS;
						this.Name = "KEYWORDS_PERFORMANCE_REPORT_" + Account_Id + "_" + Instance_Id;
						break;
					}
			}

			reportDefinition = CreateReportDefinition(selector, AccountEmails);

			// Create operations.
			ReportDefinitionOperation operation = new ReportDefinitionOperation();
			operation.operand = reportDefinition;
			operation.@operator = Operator.ADD;
			ReportDefinitionOperation[] operations = new ReportDefinitionOperation[] { operation };

			
			// Create Report Service
			var reportService = (ReportDefinitionService)User.adwordsUser.GetService(AdWordsService.v201101.ReportDefinitionService);
			//Create reportDefintions 
			ReportDefinition[] reportDefintions = reportService.mutate(operations);
			this.Id = reportDefintions[0].id;
			
			
			//========================== Retriever =======================================================
			try
			{
				// Download report.
				new ReportUtilities(User.adwordsUser).DownloadReportDefinition(reportDefintions[0].id, "c:\\testingAdwords.zip");
				Console.WriteLine("Report with definition id '{0}' was downloaded to '{1}'.",reportDefintions[0].id, "c:\\testingAdwords2.zip");
			}
			catch (Exception ex)
			{
				Console.WriteLine("Failed to download report. Exception says \"{0}\"", ex.Message);
			}
			//======================== End of Retriever =================================================
			
				

		}



		public ReportDefinition CreateReportDefinition(Selector selector, ClientSelector[] clients, DownloadFormat downloadFormat = DownloadFormat.GZIPPED_CSV)
		{
			reportDefinition.reportName = Name;
			reportDefinition.dateRangeType = dateRageType;
			
			reportDefinition.selector = selector;
			reportDefinition.downloadFormat = downloadFormat;
			reportDefinition.downloadFormatSpecified = true;
			reportDefinition.clientSelectors = clients.ToArray();
			return reportDefinition;
		}



		public string Name { get; set; }
		public long Id { get; set; }
		private GoogleUserEntity User { set; get; }
		private ClientSelector[] AccountEmails;
		public ReportDefinitionDateRangeType dateRageType { get; set; } // set from configuration 
		private ReportDefinition reportDefinition { set; get; }
		private ReportDefinitionReportType ReportType { set; get; }
        public Dictionary<string, string> FieldsMapping { set; get; }
        public DateTime StartDate { set; get; } // get from configuration
        public DateTime EndDate { set; get; }// get from configuration
        public bool includeZeroImpression { get; set; }
        public string[] selectedColumns { set; get; } // Get Selected Columns from configuration 

    }
    
}
