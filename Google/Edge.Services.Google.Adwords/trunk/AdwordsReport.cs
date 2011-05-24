using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Api.Ads.AdWords.v201101;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util;
using Edge.Data.Pipeline;
using Edge.Core.Data;
using System.Data.SqlClient;

namespace Edge.Services.Google.Adwords
{
    class AdwordsReport
    {

		static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions", "Clicks", "Cost", "CreativeDestinationUrl", "KeywordId", "Url" };
		static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost" };

        public AdwordsReport()
        {
			
        }

		public AdwordsReport(string Email, ReportDefinitionDateRangeType dateRange = ReportDefinitionDateRangeType.YESTERDAY, ReportDefinitionReportType ReportType = ReportDefinitionReportType.AD_PERFORMANCE_REPORT)
		{
			this.reportDefinition = new ReportDefinition();
			this.reportDefinition.reportType = ReportType;
			this.dateRageType = dateRange;
			//SetAccountEmails(accountEmails);
			this.User = new GoogleUserEntity(Email);
		
		}


		private void SetAccountEmails(List<string> accountEmails)
		{
			List<ClientSelector> Clients = new List<ClientSelector>();
			if ((accountEmails == null) || (accountEmails.Count == 0)) throw new Exception("Account does not contains Email list ");
			foreach (string email in accountEmails)
			{
				ClientSelector client = new ClientSelector();
				client.login = email;
				Clients.Add(client);
			}
			this.AccountEmails = Clients.ToArray();
		}


		public long intializingGoogleReport(int Account_Id , long Instance_Id)
		{
			long ReportId;
			if (!this.dateRageType.Equals(ReportDefinitionDateRangeType.CUSTOM_DATE))
			{
				ReportId = GetReportIdFromDB(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType);
			}
			else ReportId = GetReportIdFromGoogleApi(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, Instance_Id);

			return ReportId;
		}

		private long GetReportIdFromGoogleApi(int Account_Id, string p, ReportDefinitionDateRangeType reportDefinitionDateRangeType, ReportDefinitionReportType reportDefinitionReportType, long Instance_Id)
		{
			long ReportId = CreateGoogleReport(Account_Id, Instance_Id);
			SaveReportID(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType,ReportId);
			return ReportId;
		}

		private void SaveReportID(int Account_Id, string Account_Email, ReportDefinitionDateRangeType Date_Range, ReportDefinitionReportType Google_Report_Type, long ReportId)
		{
			SqlCommand cmd = DataManager.CreateCommand("SetGoogleReportDefinitionId(@Account_ID:Int, @Account_Email:Nvarchar,@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar ,@Google_Report_ID:int )", System.Data.CommandType.StoredProcedure);
			cmd.Parameters["@Account_Id"].Value = Account_Id;
			cmd.Parameters["@Account_Email"].Value = Account_Email;
			cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
			cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();
			cmd.Parameters["@Google_Report_ID"].Value = ReportId;
			
		}

		private long GetReportIdFromDB(int Account_Id, string Account_Email, ReportDefinitionDateRangeType Date_Range, ReportDefinitionReportType Google_Report_Type)
		{
			//TO DO : CHECK IF REPORT EXISTS IN DB
			long ReportId = -1 ;
			using (DataManager.Current.OpenConnection())
			{
				//SqlCommand sqlCommand = DataManager.CreateCommand("SELECT Gateway_id from UserProcess_GUI_Gateway where account_id = @AccountId:int");

				SqlCommand cmd = DataManager.CreateCommand("GetGoogleReportDefinitionId(@Account_ID:Int, @Account_Email:Nvarchar,@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar )", System.Data.CommandType.StoredProcedure);
				cmd.Parameters["@Account_Id"].Value = Account_Id;
				cmd.Parameters["@Account_Email"].Value = Account_Email;
				cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
				cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();

				using (var _reader = cmd.ExecuteReader())
				{
				    if (!_reader.IsClosed)
				        while (_reader.Read())
				        {
				            ReportId = Convert.ToInt64(_reader[0]);
				        }
				}

				if (ReportId > 0) return ReportId;
				return -1;
			}

			//if it does return report id 
			//else return -1
			throw new NotImplementedException();
		}

		public long CreateGoogleReport(int Account_Id , long Instance_Id)
		{
			
			//TO DO: Check if report exists in DB
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

			if (!this.dateRageType.Equals(ReportDefinitionDateRangeType.CUSTOM_DATE))
			{
				selector.dateRange.min = this.StartDate;
				selector.dateRange.max = this.EndDate;

			}

			// Create a filter Impressions > 0 
			Predicate predicate = new Predicate();
			predicate.field = "Impressions";
			predicate.@operator = PredicateOperator.GREATER_THAN;
			predicate.values = new string[] {"0"};
			selector.predicates = new Predicate[] { predicate };

			// Create reportDefinition
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
			
			//  TO DO : save report in DB
			//SaveReport(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, this.id);
			return reportDefintions[0].id;
			//DownloadReport(reportDefintions[0].id);

		}

		public void DownloadReport(long reportId) 
		{
			

			//========================== Retriever =======================================================
			try
			{
				// Download report.
				new ReportUtilities(User.adwordsUser).DownloadReportDefinition(reportId, "c:\\testingAdwords.zip");
				Console.WriteLine("Report with definition id '{0}' was downloaded to '{1}'.", reportId, "c:\\testingAdwords.zip");
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
			//reportDefinition.clientSelectors = clients.ToArray();
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
        public string StartDate { set; get; } // get from configuration
		public string EndDate { set; get; }// get from configuration
        public bool includeZeroImpression { get; set; }
        public string[] selectedColumns { set; get; } // Get Selected Columns from configuration 

    }
    
}
