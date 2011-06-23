using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GA = Google.Api.Ads.AdWords;
//using GA2 = Google.Api.Ads.AdWords.v201101;
//using GA = Google.Api.Ads.AdWords.Lib;
//using GA = Google.Api.Ads.AdWords.Util;
using Edge.Data.Pipeline;
using Edge.Core.Data;
using System.Data.SqlClient;
using System.Globalization;
using Edge.Core.Configuration;

namespace Edge.Services.Google.Adwords
{
	public class AdwordsReport
	{
		public long Id { get; set; }
		public GoogleUserEntity User { set; get; }
		private int _accountId { get; set; }
		private GA.v201101.ClientSelector[] AccountEmails;
		public GA.v201101.ReportDefinitionDateRangeType dateRangeType { get; set; }
		private GA.v201101.ReportDefinition reportDefinition { set; get; }
		private GA.v201101.ReportDefinitionReportType ReportType { set; get; }
		public GA.v201101.ReportDefinitionService reportService { set; get; }
		public Dictionary<string, string> FieldsMapping { set; get; } //TO DO : GET FROM CONFIGURATION
		public string StartDate { set; get; }
		public string EndDate { set; get; }
		public bool includeZeroImpression { get; set; }
		public string[] selectedColumns { set; get; } //TO DO : GET FROM CONFIGURATION 
		private GA.v201101.Selector _selector { get; set; }
		private bool _includeConversionTypes { set; get; }
		public string _customizedReportName { set; get; }

		private const string DEFAULT_ADWORDSAPI_SERVER = "https://adwords.google.com";

		static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions","Clicks", "Cost","Headline",
		                                                   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl","CampaignStatus","AccountTimeZoneId",
		                                                   "AdType","AccountCurrencyCode","Ctr","Status","AveragePosition",
		                                                   "ConversionRate","ConversionRateManyPerClick","ConversionSignificance",
		                                                   "ConversionsManyPerClick",
		                                                   "ConversionValue","TotalConvValue","ValuePerConversion","ValuePerConversionManyPerClick","ValuePerConvManyPerClick","ViewThroughConversions","ViewThroughConversionsSignificance",
		                                                   "AdNetworkType1","AdNetworkType2"
		                                               };
		static string[] AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "Id", "AdGroupId", "CampaignId", "KeywordId", "ConversionsManyPerClick", "ConversionTypeName", "ConversionCategoryName" };

		static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "CampaignId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost", "Status", "DestinationUrl", "QualityScore" };

		static string[] DESTINATION_URL_REPORT = { "AdGroupName","CampaignName","EffectiveDestinationUrl", "Impressions", "Clicks", "Cost", "ValuePerConv", "ValuePerConversion",
												   "ValuePerConversionManyPerClick", "ValuePerConvManyPerClick","ViewThroughConversions","AverageCpc","AveragePosition"};
		public AdwordsReport()
		{
			//TODO : delete this method
		}

		/// <summary>
		/// Defines report utility params for the report definition.
		/// </summary>
		/// <param name="Email">The Account Email</param>
		/// <param name="IncludeZeroImpression">In order to include zero impressions in report , set this value to be true.</param>
		/// <param name="dateRange">Report Definition Date Range Type. Default value is YESTERDAY.</param>
		/// <param name="ReportType">Report Definition Report Type. Default value is AD_PERFORMANCE_REPORT </param>
		/// <param name="includeConversionTypes">In order to create report with conversion types such as signups and purchase , set this value to be true. </param>
		public AdwordsReport(int AccountId, string Email, string StartDate, string EndDate, bool IncludeZeroImpression = false,
							GA.v201101.ReportDefinitionDateRangeType dateRange = GA.v201101.ReportDefinitionDateRangeType.YESTERDAY,
							GA.v201101.ReportDefinitionReportType ReportType = GA.v201101.ReportDefinitionReportType.AD_PERFORMANCE_REPORT,
							bool includeConversionTypes = false, string Name = "")
		{
			this._accountId = AccountId;
			this.includeZeroImpression = IncludeZeroImpression;
			this.reportDefinition = new GA.v201101.ReportDefinition();
			this.reportDefinition.reportType = ReportType;
			this.reportDefinition.dateRangeType = dateRange;
			this.ReportType = ReportType;
			this.dateRangeType = dateRange;
			this.StartDate = StartDate;
			this.EndDate = EndDate;
			//SetAccountEmails(accountEmails);
			this.User = new GoogleUserEntity(Email);
			this._includeConversionTypes = includeConversionTypes;

			//Setting customized Report Name
			if (String.IsNullOrEmpty(Name))
				this._customizedReportName = this.ReportType.ToString();
			if (_includeConversionTypes) _customizedReportName = "AD PERFORMANCE REPORT WITH CONVERSION TYPES";

			//Creating Selector
			_selector = new GA.v201101.Selector();
			switch (this.reportDefinition.reportType)
			{
				case GA.v201101.ReportDefinitionReportType.AD_PERFORMANCE_REPORT:
					{
						if (_includeConversionTypes)
							_selector.fields = AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION;
						_selector.fields = AD_PERFORMANCE_REPORT_FIELDS;
						break;
					}
				case GA.v201101.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT:
					{
						_selector.fields = KEYWORDS_PERFORMANCE_REPORT_FIELDS;
						break;
					}
				case GA.v201101.ReportDefinitionReportType.DESTINATION_URL_REPORT:
					{
						_selector.fields = DESTINATION_URL_REPORT;
						break;
					}
			}

			// Create Report Service
			reportService = (GA.v201101.ReportDefinitionService)User.adwordsUser.GetService(GA.Lib.AdWordsService.v201101.ReportDefinitionService);
		}


		private void SetAccountEmails(List<string> accountEmails)
		{
			List<GA.v201101.ClientSelector> Clients = new List<GA.v201101.ClientSelector>();
			if ((accountEmails == null) || (accountEmails.Count == 0))
				throw new Exception("Account does not contains Email list ");

			foreach (string email in accountEmails)
			{
				GA.v201101.ClientSelector client = new GA.v201101.ClientSelector();
				client.login = email;
				Clients.Add(client);
			}
			this.AccountEmails = Clients.ToArray();
		}

		/// <summary>
		/// Intializing report id from Google API / Report Definition Table in DB .
		/// </summary>
		/// <param name="Update">true in order to create new report definition, ignore exsiting report id in Report Definition Table</param>
		public long intializingGoogleReport(bool Update = false)
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
				SetReportID(this._accountId, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, ReportId, this.StartDate, this.EndDate, true);
			}

			this.Id = ReportId;
			return ReportId;
		}

		private long GetReportIdFromGoogleApi(int Account_Id, string p, GA.v201101.ReportDefinitionDateRangeType reportDefinitionDateRangeType, GA.v201101.ReportDefinitionReportType reportDefinitionReportType)
		{
			long ReportId = CreateGoogleReport(Account_Id);
			SetReportID(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, ReportId, this.StartDate, this.EndDate);
			return ReportId;
		}

		private void SetReportID(int Account_Id, string Account_Email, GA.v201101.ReportDefinitionDateRangeType Date_Range, GA.v201101.ReportDefinitionReportType Google_Report_Type,
								long ReportId, String StartDate, String EndDate, bool Update = false)
		{
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "SystemDatabase")))
			{
				SqlCommand cmd = DataManager.CreateCommand("SetGoogleReportDefinitionId(@Google_Report_Id:int,@Account_ID:Int, @Account_Email:Nvarchar" +
								",@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar ,@Google_Report_ID:int,@StartDay:Nvarchar,@EndDay:Nvarchar,@Update:bit,@reportName:Nvarchar,@reportfields:Nvarchar)"
								, System.Data.CommandType.StoredProcedure);
				sqlCon.Open();

				cmd.Connection = sqlCon;
				cmd.Parameters["@Google_Report_Id"].Value = ReportId;
				cmd.Parameters["@Account_Id"].Value = Account_Id;
				cmd.Parameters["@Account_Email"].Value = Account_Email;
				cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
				cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();
				cmd.Parameters["@reportName"].Value = _customizedReportName;

				//TO DO: create string from fields list
				cmd.Parameters["@reportfields"].Value = this._selector.fields.ToString();

				if (Date_Range.Equals(GA.v201101.ReportDefinitionDateRangeType.CUSTOM_DATE))
				{
					cmd.Parameters["@StartDay"].Value = StartDate;
					cmd.Parameters["@EndDay"].Value = EndDate;
				}
				cmd.Parameters["@Update"].Value = Update;

				cmd.ExecuteNonQuery();
			}

		}

		private long GetReportIdFromDB(int Account_Id, string Account_Email, GA.v201101.ReportDefinitionDateRangeType Date_Range,
							GA.v201101.ReportDefinitionReportType Google_Report_Type, String StartDate, String EndDate)
		{
			long ReportId = -1;
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "SystemDatabase")))
			{
				sqlCon.Open();
				//SqlCommand sqlCommand = DataManager.CreateCommand("SELECT Gateway_id from UserProcess_GUI_Gateway where account_id = @AccountId:int");

				SqlCommand cmd = DataManager.CreateCommand("GetGoogleReportDefinitionId(@Account_ID:Int, @Account_Email:Nvarchar," +
						"@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar,@StartDay:Nvarchar, @EndDay:Nvarchar,@reportName:Nvarchar,@reportfields:Nvarchar )", System.Data.CommandType.StoredProcedure);
				cmd.Connection = sqlCon;

				cmd.Parameters["@Account_Id"].Value = Account_Id;
				cmd.Parameters["@Account_Email"].Value = Account_Email;
				cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
				cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();
				if (Date_Range.Equals(GA.v201101.ReportDefinitionDateRangeType.CUSTOM_DATE))
				{
					cmd.Parameters["@StartDay"].Value = StartDate;
					cmd.Parameters["@EndDay"].Value = EndDate;
				}
				cmd.Parameters["@reportName"].Value = _customizedReportName;

				//TO DO: create JSON obect from fields array
				//	cmd.Parameters["@reportfields"].Value = this._selector.fields.

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
			if (this.dateRangeType.Equals(GA.v201101.ReportDefinitionDateRangeType.CUSTOM_DATE))
			{
				_selector.dateRange = new GA.v201101.DateRange()
				{
					min = this.StartDate,
					max = this.EndDate
				};

			}

			if (!this.includeZeroImpression && !_includeConversionTypes)
			{
				// Create a filter Impressions > 0 
				GA.v201101.Predicate impPredicate = new GA.v201101.Predicate();
				impPredicate.field = "Impressions";
				impPredicate.@operator = GA.v201101.PredicateOperator.GREATER_THAN;
				impPredicate.values = new string[] { "0" };
				_selector.predicates = new GA.v201101.Predicate[] { impPredicate };
				
				//Sorting 
				//if (this.ReportType.Equals(GA.v201101.ReportDefinitionReportType.AD_PERFORMANCE_REPORT))
				//{
				//    _selector.ordering = new GA.v201101.OrderBy[]
				//    {
				//        new GA.v201101.OrderBy() { field = "Id", sortOrder = GA.v201101.SortOrder.DESCENDING },
				//        new GA.v201101.OrderBy() { field = "KeywordId", sortOrder = GA.v201101.SortOrder.DESCENDING }
				//    };
				//}
			}

			// Create reportDefinition
			reportDefinition = CreateReportDefinition(_selector, AccountEmails);

			// Create operations.
			GA.v201101.ReportDefinitionOperation operation = new GA.v201101.ReportDefinitionOperation();
			operation.operand = reportDefinition;
			operation.@operator = GA.v201101.Operator.ADD;
			GA.v201101.ReportDefinitionOperation[] operations = new GA.v201101.ReportDefinitionOperation[] { operation };

			//Create reportDefintions 
			GA.v201101.ReportDefinition[] reportDefintions = reportService.mutate(operations);
			this.Id = reportDefintions[0].id;

			//  TO DO : save report in DB
			//SaveReport(Account_Id, this.User.email, this.reportDefinition.dateRangeType, this.reportDefinition.reportType, this.id);
			return reportDefintions[0].id;
			//DownloadReport(reportDefintions[0].id);

		}

		public void AddFilter(string fieldName, GA.v201101.PredicateOperator op, string[] values)
		{
			GA.v201101.Predicate predicate = new GA.v201101.Predicate();
			predicate.field = fieldName;
			predicate.@operator = op;
			predicate.values = values;
			if (_selector.predicates == null)
				_selector.predicates = new GA.v201101.Predicate[] { predicate };
			else
			{
				List<GA.v201101.Predicate> predicatesList = _selector.predicates.ToList<GA.v201101.Predicate>();
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

		public GA.v201101.ReportDefinition CreateReportDefinition(GA.v201101.Selector selector, GA.v201101.ClientSelector[] clients,
							GA.v201101.DownloadFormat downloadFormat = GA.v201101.DownloadFormat.GZIPPED_CSV)
		{
			reportDefinition.reportName = _customizedReportName;
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
				new GA.Util.ReportUtilities(User.adwordsUser).DownloadReportDefinition(reportId, Path);
			}
			catch (Exception ex)
			{
				throw new Exception("Failed to download report. Exception says" + ex.Message);
			}
			//======================== End of Retriever =================================================
		}

	}

}
