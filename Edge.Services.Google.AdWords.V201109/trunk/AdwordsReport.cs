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
using Newtonsoft.Json;
using Google.Api.Ads.AdWords.v201109;
using Google.Api.Ads.AdWords.Lib;
using Edge.Core.Utilities;
using System.Net;
using System.Web;
using Google.Api.Ads.Common.Util;
using System.Xml;

namespace Edge.Services.Google.AdWords
{
	public class AdwordsReport
	{
		public long Id { get; set; }
		public GoogleUserEntity user { set; get; }
		private int _accountId { get; set; }
		//private GA.v201101.ClientSelector[] _accountEmails;
		public GA.v201109.ReportDefinitionDateRangeType dateRangeType { get; set; }

		public GA.v201109.ReportDefinition _reportDefinition { set; get; }

		private GA.v201109.ReportDefinitionReportType _reportType { set; get; }
		public GA.v201109.ReportDefinitionService reportService { set; get; }
		public Dictionary<string, string> fieldsMapping { set; get; } //TO DO : GET FROM CONFIGURATION
		public string startDate { set; get; }
		public string endDate { set; get; }
		public bool includeZeroImpression { get; set; }
		public string[] selectedColumns { set; get; } //TO DO : GET FROM CONFIGURATION 
		private GA.v201109.Selector _selector { get; set; }
		private bool _includeConversionTypes { set; get; }
		public string customizedReportName { set; get; }
		private string _reportEmail { set; get; }
		public string _adwordsClientId { get; set; }
		private string _reportMcc { set; get; }


		private const string DEFAULT_ADWORDSAPI_SERVER = "https://adwords.google.com";
		private const string ADHOC_REPORT_URL_FORMAT = "{0}/api/adwords/reportdownload/v201109";
		private const string REPORT_URL_FORMAT = "{0}/api/adwords/reportdownload?__rd={1}";

		#region Supported Reports fields
		string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions","Clicks", "Cost","Headline",
		                                                   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl","CampaignStatus","AccountTimeZoneId",
		                                                   "AdType","AccountCurrencyCode","Ctr","Status","AveragePosition","Conversions",
		                                                   "ConversionRate","ConversionRateManyPerClick","ConversionSignificance",
		                                                   "ConversionsManyPerClick",
		                                                   "ConversionValue","TotalConvValue","ValuePerConversion","ValuePerConversionManyPerClick","ValuePerConvManyPerClick","ViewThroughConversions","ViewThroughConversionsSignificance",
		                                                   "AdNetworkType1"
		                                               };
		 string[] AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "Id", "KeywordId", "ConversionsManyPerClick", "ConversionCategoryName" };

		 string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "CampaignId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost", "Status", "DestinationUrl", "QualityScore" };

		 string[] DESTINATION_URL_REPORT = { "AdGroupName","CampaignName","EffectiveDestinationUrl", "Impressions", "Clicks", "Cost", "ValuePerConv", "ValuePerConversion",
												   "ValuePerConversionManyPerClick", "ValuePerConvManyPerClick","ViewThroughConversions","AverageCpc","AveragePosition"};

		 string[] MANAGED_PLACEMENTS_PERFORMANCE_REPORT = { "Id", "CampaignId", "AdGroupId", "DestinationUrl", "PlacementUrl", "Status" };

		 string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT = { "Id", "CampaignId", "AdGroupId", "CriteriaParameters", "Domain" };
		#endregion Reports fields

		/// <summary>
		/// Defines report utility params for the report definition.
		/// </summary>
		/// <param name="MccEmail">The Account Email</param>
		/// <param name="IncludeZeroImpression">In order to include zero impressions in report , set this value to be true.</param>
		/// <param name="dateRange">Report Definition Date Range Type. Default value is YESTERDAY.</param>
		/// <param name="ReportType">Report Definition Report Type. Default value is AD_PERFORMANCE_REPORT </param>
		/// <param name="includeConversionTypes">In order to create report with conversion types such as signups and purchase , set this value to be true. </param>
		public AdwordsReport(int AccountId, string mccEmail, string AdwordsClientId, string StartDate, string EndDate, bool IncludeZeroImpression = false,
							GA.v201109.ReportDefinitionDateRangeType dateRange = GA.v201109.ReportDefinitionDateRangeType.YESTERDAY,
							GA.v201109.ReportDefinitionReportType ReportType = GA.v201109.ReportDefinitionReportType.AD_PERFORMANCE_REPORT,
							bool includeConversionTypes = false, string Name = "")
		{
			this._accountId = AccountId;
			this.includeZeroImpression = IncludeZeroImpression;
			this._reportDefinition = new GA.v201109.ReportDefinition();
			this._reportDefinition.reportType = ReportType;
			this._reportDefinition.dateRangeType = dateRange;
			this._reportType = ReportType;
			this.dateRangeType = dateRange;
			this.startDate = StartDate;
			this.endDate = EndDate;
			//this._reportEmail = accountEmail;
			this._adwordsClientId = AdwordsClientId;
			this._reportMcc = mccEmail;
			//SetAccountEmails(accountEmails);
			this.user = new GoogleUserEntity(_reportMcc, _adwordsClientId);
			this._includeConversionTypes = includeConversionTypes;

			//Setting customized Report Name
			if (String.IsNullOrEmpty(Name))
				this.customizedReportName = this._reportType.ToString();
			if (_includeConversionTypes) customizedReportName = "AD PERFORMANCE REPORT WITH CONVERSION TYPES";

			//Creating Selector
			_selector = new GA.v201109.Selector();
			switch (this._reportDefinition.reportType)
			{
				case GA.v201109.ReportDefinitionReportType.AD_PERFORMANCE_REPORT:
					{
						if (_includeConversionTypes)
							_selector.fields = AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION;
						else
							_selector.fields = AD_PERFORMANCE_REPORT_FIELDS;
						break;


					}
				case GA.v201109.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT:
					{
						_selector.fields = KEYWORDS_PERFORMANCE_REPORT_FIELDS;
						break;
					}
				case GA.v201109.ReportDefinitionReportType.DESTINATION_URL_REPORT:
					{
						_selector.fields = DESTINATION_URL_REPORT;
						break;
					}
				case GA.v201109.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT:
					{
						_selector.fields = MANAGED_PLACEMENTS_PERFORMANCE_REPORT;
						break;
					}

				case GA.v201109.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT:
					{
						_selector.fields = AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT;
						break;
					}
			}

			// Create Report Service
			reportService = CreateReportService(user);
		}

		private GA.v201109.ReportDefinitionService CreateReportService(GoogleUserEntity googleUserEntity)
		{
			return (GA.v201109.ReportDefinitionService)googleUserEntity.AdwordsUser.GetService(GA.Lib.AdWordsService.v201109.ReportDefinitionService);
		}


		//EMAILS ADDRESS ARE NOT SUPPORTED IN VERSION 201109
		//private void SetAccountEmails(List<string> accountEmails)
		//{
		//    List<GA.v201101.ClientSelector> Clients = new List<GA.v201101.ClientSelector>();
		//    if ((accountEmails == null) || (accountEmails.Count == 0))
		//        throw new Exception("Account does not contains Email list ");

		//    foreach (string email in accountEmails)
		//    {
		//        GA.v201101.ClientSelector client = new GA.v201101.ClientSelector();
		//        client.login = email;
		//        Clients.Add(client);
		//    }
		//    this._accountEmails = Clients.ToArray();
		//}

		/// <summary>
		/// Intializing report id from Google API / Report Definition Table in DB .
		/// </summary>
		/// <param name="Update">true in order to create new report definition, ignore exsiting report id in Report Definition Table</param>
		//public long intializingGoogleReport(bool InvalidReportId = false , bool Retry = false)
		//{
		//    long ReportId;
		//    if (!InvalidReportId)
		//    {
		//        ReportId = GetReportIdFromDB(this._accountId, this._adwordsClientId, this.dateRangeType, this._reportType, this.startDate, this.endDate);
		//        if (ReportId == -1)
		//            ReportId = GetReportIdFromGoogleApi(this._accountId, this._adwordsClientId,this.dateRangeType, this._reportType, Retry);
		//    }
		//    else // Invalid Report Id - get from API
		//    {
		//        ReportId = GetReportIdFromGoogleApi(this._accountId, this._adwordsClientId, this.dateRangeType, this._reportType, Retry);
		//        SetReportID(this._accountId, this._adwordsClientId, this._reportDefinition.dateRangeType, this._reportDefinition.reportType, ReportId, this.startDate, this.endDate, true);
		//    }

		//    this.Id = ReportId;
		//    return ReportId;
		//}

		//private long GetReportIdFromGoogleApi(int Account_Id, string GoogleUniqueID, GA.v201109.ReportDefinitionDateRangeType reportDefinitionDateRangeType, GA.v201109.ReportDefinitionReportType reportDefinitionReportType, bool CreateNewAuth)
		//{
		//    long ReportId = CreateGoogleReport(Account_Id, CreateNewAuth);
		//    SetReportID(Account_Id, GoogleUniqueID, this._reportDefinition.dateRangeType, this._reportDefinition.reportType, ReportId, this.startDate, this.endDate);
		//    return ReportId;
		//}

		//private void SetReportID(int Account_Id, string GoogleUniqueID, GA.v201109.ReportDefinitionDateRangeType Date_Range, GA.v201109.ReportDefinitionReportType Google_Report_Type,
		//                        long ReportId, String StartDate, String EndDate, bool Update = false)
		//{
		//    using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "SystemDatabase")))
		//    {
		//        SqlCommand cmd = DataManager.CreateCommand("SetGoogleReportDefinitionId(@Google_Report_Id:int,@Account_ID:Int, @GoogleUniqueID:Nvarchar" +
		//                        ",@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar ,@Google_Report_ID:int,@StartDay:Nvarchar,@EndDay:Nvarchar,@Update:bit,@reportName:Nvarchar,@reportfields:Nvarchar)"
		//                        , System.Data.CommandType.StoredProcedure);
		//        sqlCon.Open();

		//        cmd.Connection = sqlCon;
		//        cmd.Parameters["@Google_Report_Id"].Value = ReportId;
		//        cmd.Parameters["@Account_Id"].Value = Account_Id;
		//        cmd.Parameters["@GoogleUniqueID"].Value = GoogleUniqueID;
		//        cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
		//        cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();
		//        cmd.Parameters["@reportName"].Value = customizedReportName;

		//        //TO DO: create string from fields list
		//        cmd.Parameters["@reportfields"].Size = 1000;
		//        cmd.Parameters["@reportfields"].Value = JsonConvert.SerializeObject(this._selector.fields, Formatting.None);

		//        if (Date_Range.Equals(GA.v201109.ReportDefinitionDateRangeType.CUSTOM_DATE))
		//        {
		//            cmd.Parameters["@StartDay"].Value = StartDate;
		//            cmd.Parameters["@EndDay"].Value = EndDate;
		//        }
		//        cmd.Parameters["@Update"].Value = Update;

		//        cmd.ExecuteNonQuery();
		//    }

		//}

		//private long GetReportIdFromDB(int Account_Id, string GoogleUniqueID, GA.v201109.ReportDefinitionDateRangeType Date_Range,
		//                    GA.v201109.ReportDefinitionReportType Google_Report_Type, String StartDate, String EndDate)
		//{
		//    long ReportId = -1;
		//    using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "SystemDatabase")))
		//    {
		//        sqlCon.Open();
		//        //SqlCommand sqlCommand = DataManager.CreateCommand("SELECT Gateway_id from UserProcess_GUI_Gateway where account_id = @AccountId:int");

		//        SqlCommand cmd = DataManager.CreateCommand("GetGoogleReportDefinitionId(@Account_ID:Int, @GoogleUniqueID:Nvarchar," +
		//                "@Date_Range:Nvarchar, @Google_Report_Type:Nvarchar,@StartDay:Nvarchar, @EndDay:Nvarchar,@reportName:Nvarchar,@reportfields:Nvarchar )", System.Data.CommandType.StoredProcedure);
		//        cmd.Connection = sqlCon;

		//        cmd.Parameters["@Account_Id"].Value = Account_Id;
		//        cmd.Parameters["@GoogleUniqueID"].Value = GoogleUniqueID;
		//        cmd.Parameters["@Date_Range"].Value = Date_Range.ToString();
		//        cmd.Parameters["@Google_Report_Type"].Value = Google_Report_Type.ToString();
		//        if (Date_Range.Equals(GA.v201109.ReportDefinitionDateRangeType.CUSTOM_DATE))
		//        {
		//            cmd.Parameters["@StartDay"].Value = StartDate;
		//            cmd.Parameters["@EndDay"].Value = EndDate;
		//        }
		//        cmd.Parameters["@reportName"].Value = customizedReportName;
		//        cmd.Parameters["@reportfields"].Size = 1000;
		//        cmd.Parameters["@reportfields"].Value = JsonConvert.SerializeObject(this._selector.fields, Formatting.None);

		//        using (var _reader = cmd.ExecuteReader())
		//        {
		//            if (!_reader.IsClosed)
		//            {
		//                while (_reader.Read())
		//                {
		//                    ReportId = Convert.ToInt64(_reader[0]);
		//                }

		//            }
		//        }

		//        if (ReportId > 0) return ReportId;
		//        return -1;
		//    }
		//}

		public void AddFilter(string fieldName, GA.v201109.PredicateOperator op, string[] values)
		{
			GA.v201109.Predicate predicate = new GA.v201109.Predicate();
			predicate.field = fieldName;
			predicate.@operator = op;
			predicate.values = values;
			if (_selector.predicates == null)
				_selector.predicates = new GA.v201109.Predicate[] { predicate };
			else
			{
				List<GA.v201109.Predicate> predicatesList = _selector.predicates.ToList<GA.v201109.Predicate>();
				predicatesList.Add(predicate);
				_selector.predicates = predicatesList.ToArray();
			}
		}
		public long CreateGoogleReport()
		{

			#region TimePeriod
			if (this.dateRangeType.Equals(GA.v201109.ReportDefinitionDateRangeType.CUSTOM_DATE))
			{
				_selector.dateRange = new GA.v201109.DateRange()
				{
					min = this.startDate,
					max = this.endDate
				};

			}
			#endregion

			#region Filltering Report
			if (!this.includeZeroImpression && !_includeConversionTypes)
			{
				// Create a filter Impressions > 0 
				GA.v201109.Predicate impPredicate = new GA.v201109.Predicate();
				impPredicate.field = "Impressions";
				impPredicate.@operator = GA.v201109.PredicateOperator.GREATER_THAN;
				impPredicate.values = new string[] { "0" };
				_selector.predicates = new GA.v201109.Predicate[] { impPredicate };
			}

			#endregion

			// Create reportDefinition
			_reportDefinition = CreateReportDefinition(_selector);

			// Create operations.
			//GA.v201109.ReportDefinitionOperation operation = new GA.v201109.ReportDefinitionOperation();
			//operation.operand = _reportDefinition;
			//operation.@operator = GA.v201109.Operator.ADD;
			//GA.v201109.ReportDefinitionOperation[] operations = new GA.v201109.ReportDefinitionOperation[] { operation };

			////Create Report 
			//try
			//{
			//    GA.v201109.ReportDefinition[] reportDefintions = reportService.mutate(operations);
			//    this.Id = reportDefintions[0].id;
			//}
			////AuthenticationError Handle 
			//catch (AdWordsApiException ex) 
			//{
			//    if (AuthRetry)
			//    {
			//        Log.Write("Google Authentication Error:  Cannot Renew Authentication", ex);
			//        throw new Exception("Google Authentication Error:  Cannot Renew Authentication", ex);
			//    }
			//    else
			//    {
			//        //Creating new Authentication
			//        this.user = new GoogleUserEntity(this._reportMcc, this._adwordsClientId, true);
			//        this.reportService = CreateReportService(this.user);
			//        Log.Write("AuthenticationError.GOOGLE_ACCOUNT_COOKIE_INVALID, Authentication has been renewed : ", ex);
			//        throw ex;
			//    }
			//}
			//catch (Exception e)
			//{
			//    Log.Write("Exception has occured while trying to create new reportDefintions", e);
			//    throw e;
			//}
			return this.Id;
		}



		//TODO: check what is IsReturnMoneyInMicros param in google
		public GoogleRequestEntity GetReportUrlParams(bool IsReturnMoneyInMicros = true)
		{
			AdWordsAppConfig config = (AdWordsAppConfig)this.user.AdwordsUser.Config;

			string postBody = "__rdxml=" + HttpUtility.UrlEncode(ConvertDefinitionToXml(this._reportDefinition)); ;
			string downloadUrl = string.Format(ADHOC_REPORT_URL_FORMAT, config.AdWordsApiServer);

			return new GoogleRequestEntity(
			downloadUrl,
			this.user.AdwordsClientId,
			config.ClientEmail,
			config.AuthToken,
			IsReturnMoneyInMicros,
			config.DeveloperToken,
			postBody,
			config.EnableGzipCompression.ToString()

			);
			//return new GoogleRequestEntity(
			//    downloadUrl,
			//    reportService.RequestHeader.clientCustomerId,
			//    reportService.RequestHeader.clientEmail,
			//    reportService.RequestHeader.authToken,
			//    IsReturnMoneyInMicros,
			//    reportService.RequestHeader.developerToken,
			//    postBody,
			//    config.EnableGzipCompression.ToString()

			//    );
		}


		//public WebRequest CreateWebRequest(out string requestBody)
		//{

		//    AdWordsAppConfig config = (AdWordsAppConfig)this.user.AdwordsUser.Config;

		//    string postBody = null;
		//    string downloadUrl;

		//    downloadUrl = string.Format(ADHOC_REPORT_URL_FORMAT, config.AdWordsApiServer);
		//    postBody = "__rdxml=" + HttpUtility.UrlEncode(ConvertDefinitionToXml(this._reportDefinition));

		//    WebRequest request = HttpWebRequest.Create(downloadUrl);
		//    request.Method = "POST";
		//    request.Headers.Add("clientEmail: " + config.ClientEmail);
		//    request.Headers.Add("clientCustomerId: " + config.ClientCustomerId);
		//    request.ContentType = "application/x-www-form-urlencoded";
		//    request.Headers["Authorization"] = "GoogleLogin auth=" + this.reportService.RequestHeader.authToken;
		//    request.Headers.Add("returnMoneyInMicros: true");
		//    request.Headers.Add("developerToken: " + config.DeveloperToken);


		//    if (config.EnableGzipCompression)
		//    {
		//        (request as HttpWebRequest).AutomaticDecompression = DecompressionMethods.GZip
		//            | DecompressionMethods.Deflate;
		//    }
		//    else
		//    {
		//        (request as HttpWebRequest).AutomaticDecompression = DecompressionMethods.None;
		//    }


		//    requestBody = postBody;
		//    return request;

		//}

		private string ConvertDefinitionToXml<T>(T definition)
		{
			string xml = SerializationUtilities.SerializeAsXmlText(definition).Replace(
				"ReportDefinition", "reportDefinition");
			XmlDocument doc = new XmlDocument();
			doc.LoadXml(xml);
			XmlNodeList xmlNodes = doc.SelectNodes("descendant::*");
			foreach (XmlElement node in xmlNodes)
			{
				node.RemoveAllAttributes();
			}
			return doc.OuterXml;
		}

		public GA.v201109.ReportDefinition CreateReportDefinition(GA.v201109.Selector selector,
							GA.v201109.DownloadFormat downloadFormat = GA.v201109.DownloadFormat.GZIPPED_CSV)
		{
			_reportDefinition.reportName = customizedReportName;
			_reportDefinition.dateRangeType = dateRangeType;

			_reportDefinition.selector = selector;
			_reportDefinition.downloadFormat = downloadFormat;
			_reportDefinition.downloadFormatSpecified = true;
			return _reportDefinition;
		}





	}

}
