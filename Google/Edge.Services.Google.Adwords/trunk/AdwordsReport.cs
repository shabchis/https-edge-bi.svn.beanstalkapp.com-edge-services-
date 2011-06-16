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
using System.Globalization;
using Edge.Core.Configuration;

namespace Edge.Services.Google.Adwords
{
	public class AdwordsReport
	{
		public string Name { get; set; }
		public long Id { get; set; }
		public GoogleUserEntity User { set; get; }
		private int _accountId { get; set; }
		private ClientSelector[] AccountEmails;
		public ReportDefinitionDateRangeType dateRangeType { get; set; }
		private ReportDefinition reportDefinition { set; get; }
		private ReportDefinitionReportType ReportType { set; get; }
		public ReportDefinitionService reportService { set; get; }
		public Dictionary<string, string> FieldsMapping { set; get; } //TO DO : GET FROM CONFIGURATION
		public string StartDate { set; get; }
		public string EndDate { set; get; }
		public bool includeZeroImpression { get; set; }
		public string[] selectedColumns { set; get; } //TO DO : GET FROM CONFIGURATION 
		private Selector _selector { get; set; }
		
		private const string DEFAULT_ADWORDSAPI_SERVER = "https://adwords.google.com";
		static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions", "Clicks", "Cost","Headline",
														   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl" };
		static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "CampaignId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost", "Status" };
		static string[] DESTINATION_URL_REPORT = { "AdGroupName","CampaignName","EffectiveDestinationUrl", "Impressions", "Clicks", "Cost", "ValuePerConv", "ValuePerConversion",
													 "ValuePerConversionManyPerClick", "ValuePerConvManyPerClick","ViewThroughConversions","AverageCpc","AveragePosition"};
		public AdwordsReport()
		{
			//TODO : delete this method
		}

		public AdwordsReport(int AccountId, string Email,string StartDate, string EndDate, bool IncludeZeroImpression = false,
							ReportDefinitionDateRangeType dateRange = ReportDefinitionDateRangeType.YESTERDAY,
							ReportDefinitionReportType ReportType = ReportDefinitionReportType.AD_PERFORMANCE_REPORT)
		{
			this._accountId = AccountId;
			this.includeZeroImpression = IncludeZeroImpression;
			this.reportDefinition = new ReportDefinition();
			this.reportDefinition.reportType = ReportType;
			this.reportDefinition.dateRangeType = dateRange;
			this.ReportType = ReportType;
			this.dateRangeType = dateRange;
			this.StartDate = StartDate;
			this.EndDate = EndDate;
			//SetAccountEmails(accountEmails);
			this.User = new GoogleUserEntity(Email);

			//Creating Selector
			_selector = new Selector();

			// Create Report Service
			reportService = (ReportDefinitionService)User.adwordsUser.GetService(AdWordsService.v201101.ReportDefinitionService);
			this.Name = ReportType.ToString();
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


		public long intializingGoogleReport(bool Update  = false)
		{
			long ReportId;
			if (!Update)
			{
				ReportId = GetReportIdFromDB(this._accountId, this.User.email, this.dateRangeType, this.ReportType, this.StartDate, this.EndDate);
				if (ReportId == -1)
					ReportId = GetReportIdFromGoogleApi(this._accountId, this.User.email, this.dateRangeType, this.ReportType);
			}
			else
			{
				ReportId = GetReportIdFromGoogleApi(this._accountId, this.User.email, this.dateRangeType, this.ReportType);
				SetReportID(this._accountId, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, ReportId, this.StartDate, this.EndDate,true);
			}

			this.Id = ReportId;
			return ReportId;
		}

		private long GetReportIdFromGoogleApi(int Account_Id, string p, ReportDefinitionDateRangeType reportDefinitionDateRangeType, ReportDefinitionReportType reportDefinitionReportType)
		{
			long ReportId = CreateGoogleReport(Account_Id);
			SetReportID(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, ReportId, this.StartDate, this.EndDate);
			return ReportId;
		}

		private void SetReportID(int Account_Id, string Account_Email, ReportDefinitionDateRangeType Date_Range, ReportDefinitionReportType Google_Report_Type,
								long ReportId, String StartDate, String EndDate, bool Update = false)
		{
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "SystemDatabase")))
			{
				SqlCommand cmd = DataManager.CreateCommand("SetGoogleReportDefinitionId(@Google_Report_Id:int,@Account_ID:Int, @Account_Email:Nvarchar" +
								",@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar ,@Google_Report_ID:int,@StartDay:Nvarchar,@EndDay:Nvarchar,@Update:bit )", System.Data.CommandType.StoredProcedure);
				sqlCon.Open();

				cmd.Connection = sqlCon;
				cmd.Parameters["@Google_Report_Id"].Value = ReportId;
				cmd.Parameters["@Account_Id"].Value = Account_Id;
				cmd.Parameters["@Account_Email"].Value = Account_Email;
				cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
				cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();

				if (Date_Range.Equals(ReportDefinitionDateRangeType.CUSTOM_DATE))
				{
					cmd.Parameters["@StartDay"].Value = StartDate;
					cmd.Parameters["@EndDay"].Value = EndDate;
				}
				cmd.Parameters["@Update"].Value = Update;

				cmd.ExecuteNonQuery();
			}

		}
		
		private long GetReportIdFromDB(int Account_Id, string Account_Email, ReportDefinitionDateRangeType Date_Range, ReportDefinitionReportType Google_Report_Type, String StartDate, String EndDate)
		{
			long ReportId = -1;
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "SystemDatabase")))
			{
				sqlCon.Open();
				//SqlCommand sqlCommand = DataManager.CreateCommand("SELECT Gateway_id from UserProcess_GUI_Gateway where account_id = @AccountId:int");

				SqlCommand cmd = DataManager.CreateCommand("GetGoogleReportDefinitionId(@Account_ID:Int, @Account_Email:Nvarchar," +
						"@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar,@StartDay:Nvarchar, @EndDay:Nvarchar )", System.Data.CommandType.StoredProcedure);
				cmd.Connection = sqlCon;

				cmd.Parameters["@Account_Id"].Value = Account_Id;
				cmd.Parameters["@Account_Email"].Value = Account_Email;
				cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
				cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();
				if (Date_Range.Equals(ReportDefinitionDateRangeType.CUSTOM_DATE))
				{
					cmd.Parameters["@StartDay"].Value = StartDate;
					cmd.Parameters["@EndDay"].Value = EndDate;
				}

				using (var _reader = cmd.ExecuteReader())
				{
					if (!_reader.IsClosed)
					{
						while (_reader.Read())
						{
							ReportId = Convert.ToInt64(_reader[0]);
						}
						
					}
				}

				if (ReportId > 0) return ReportId;
				return -1;
			}
		}

		public long CreateGoogleReport(int Account_Id)
		{

			

			switch (this.reportDefinition.reportType)
			{
				case ReportDefinitionReportType.AD_PERFORMANCE_REPORT:
					{
						_selector.fields = AD_PERFORMANCE_REPORT_FIELDS;
						
						break;
					}
				case ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT:
					{
						_selector.fields = KEYWORDS_PERFORMANCE_REPORT_FIELDS;
						break;
					}
				case ReportDefinitionReportType.DESTINATION_URL_REPORT:
					{
						_selector.fields = DESTINATION_URL_REPORT;
						break;
					}
			}

			this.Name = this.reportDefinition.reportType.ToString() + Account_Id;

			if (this.dateRangeType.Equals(ReportDefinitionDateRangeType.CUSTOM_DATE))
			{
				_selector.dateRange = new DateRange()
				{
					min = this.StartDate,
					max = this.EndDate
				};
				
			}


			if (!this.includeZeroImpression)
			{
				// Create a filter Impressions > 0 
				Predicate predicate = new Predicate();
				predicate.field = "Impressions";
				predicate.@operator = PredicateOperator.GREATER_THAN;
				predicate.values = new string[] { "0" };
				_selector.predicates = new Predicate[] { predicate };
			}
			
			// Create reportDefinition
			reportDefinition = CreateReportDefinition(_selector, AccountEmails);

			// Create operations.
			ReportDefinitionOperation operation = new ReportDefinitionOperation();
			operation.operand = reportDefinition;
			operation.@operator = Operator.ADD;
			ReportDefinitionOperation[] operations = new ReportDefinitionOperation[] { operation };


			

			//Create reportDefintions 
			ReportDefinition[] reportDefintions = reportService.mutate(operations);
			this.Id = reportDefintions[0].id;

			//  TO DO : save report in DB
			//SaveReport(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, this.id);
			return reportDefintions[0].id;
			//DownloadReport(reportDefintions[0].id);

		}
		public void AddFilter(string fieldName, PredicateOperator op, string[] values )
		{
			Predicate predicate = new Predicate();
			predicate.field = fieldName;
			predicate.@operator = op;
			predicate.values = values;
			if (_selector.predicates == null)
				_selector.predicates = new Predicate[] { predicate };
			else
			{
				List<Predicate> predicatesList = _selector.predicates.ToList<Predicate>();
				predicatesList.Add(predicate);
				_selector.predicates = predicatesList.ToArray();
			}
		}

		//TODO: check what is IsReturnMoneyInMicros param in google
		public GoogleRequestEntity GetReportUrlParams(bool IsReturnMoneyInMicros = true)
		{
			return new GoogleRequestEntity(
				new Uri(string.Format(CultureInfo.InvariantCulture, "{0}/api/adwords/reportdownload?__rd={1}", DEFAULT_ADWORDSAPI_SERVER, this.Id)),
				reportService.RequestHeader.clientCustomerId,
				reportService.RequestHeader.clientEmail,
				reportService.RequestHeader.authToken,
				IsReturnMoneyInMicros);
		}

		public ReportDefinition CreateReportDefinition(Selector selector, ClientSelector[] clients, DownloadFormat downloadFormat = DownloadFormat.GZIPPED_CSV)
		{
			reportDefinition.reportName = Name;
			reportDefinition.dateRangeType = dateRangeType;

			reportDefinition.selector = selector;
			reportDefinition.downloadFormat = downloadFormat;
			reportDefinition.downloadFormatSpecified = true;
			//reportDefinition.clientSelectors = clients.ToArray();
			return reportDefinition;
		}


		public void DownloadReport(long reportId, string Path = @"c:\testingAdwords.zip")
		{

			//========================== Retriever =======================================================
			try
			{
				// Download report.
				new ReportUtilities(User.adwordsUser).DownloadReportDefinition(reportId, Path);
			}
			catch (Exception ex)
			{
				throw new Exception("Failed to download report. Exception says" + ex.Message);
			}
			//======================== End of Retriever =================================================
		}



		
	}

}
