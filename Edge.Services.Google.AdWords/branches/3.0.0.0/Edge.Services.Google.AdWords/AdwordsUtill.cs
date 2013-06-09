using System;
using System.Collections.Generic;
using GA = Google.Api.Ads.AdWords;
using ADWORDS_V201302 = Google.Api.Ads.AdWords.v201302;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Utilities;
using Google.Api.Ads.Common.Lib;

namespace Edge.Services.Google.AdWords
{
	public static class AdwordsUtill
	{
		public static string GetAuthToken(GA.Lib.AdWordsUser user, bool generateNew = false)
		{
			string pass;
			string auth;

			auth = GetAuthFromDB((user.Config as GA.Lib.AdWordsAppConfig).Email, out pass);

			//Set User Password
			(user.Config as GA.Lib.AdWordsAppConfig).Password = pass;

			if (generateNew)
				auth = GetAuthFromApi(user);

			return string.IsNullOrEmpty(auth) ? GetAuthFromApi(user) : auth;
		}

		private static string GetAuthFromApi(GA.Lib.AdWordsUser user)
		{
			string auth;
			try
			{
				auth = new AuthToken(
					   (user.Config as GA.Lib.AdWordsAppConfig),
					   GA.Lib.AdWordsSoapClient.SERVICE_NAME,
					   (user.Config as GA.Lib.AdWordsAppConfig).Email,
					   (user.Config as GA.Lib.AdWordsAppConfig).Password).GetToken();

				SaveAuthTokenToDB((user.Config as GA.Lib.AdWordsAppConfig).Email, auth);
			}
			catch (Exception ex)
			{
				Log.Write("AdwordsUtill", "Error while trying to create new Auth key", ex);
				throw new Exception("Error while trying to create new Auth key", ex);
			}
			return auth;
		}

		private static void SaveAuthTokenToDB(string mccEmail, string authToken)
		{
			var connection = new SqlConnection(AppSettings.GetConnectionString(typeof(AdwordsUtill), "MCC_Auth"));
			try
			{
				using (connection)
				{
					var cmd = SqlUtility.CreateCommand(@"SetGoogleMccAuth(@MccEmail:Nvarchar,@AuthToken:Nvarchar)", System.Data.CommandType.StoredProcedure);
					cmd.Connection = connection;
					connection.Open();
					cmd.Parameters["@MccEmail"].Value = mccEmail;
					cmd.Parameters["@AuthToken"].Value = authToken;
					cmd.ExecuteNonQuery();
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error while trying to set a new Auth key", ex);
			}
		}

		private static string GetAuthFromDB(string mccEmail, out string mccPassword)
		{
			var auth = "";
			mccPassword = "";
			var connection = new SqlConnection(AppSettings.GetConnectionString(typeof(AdwordsUtill), "MCC_Auth"));
			try
			{
				using (connection)
				{
					var cmd = SqlUtility.CreateCommand(@"GetGoogleMccAuth(@MccEmail:Nvarchar)", System.Data.CommandType.StoredProcedure);
					cmd.Connection = connection;
					connection.Open();
					cmd.Parameters["@MccEmail"].Value = mccEmail;

					using (SqlDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							mccPassword = Encryptor.Dec(reader[0].ToString());
							auth = reader[1].ToString();
						}
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error while trying to get auth key from DB", ex);
			}
			return auth;
		}
	}

	public static class GoogleStaticReportsNamesUtill
	{
		public static Dictionary<ADWORDS_V201302.ReportDefinitionReportType, string> ReportNames = new Dictionary<ADWORDS_V201302.ReportDefinitionReportType, string>
			{
			{ADWORDS_V201302.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT, "KEYWORDS_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, "AD_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.URL_PERFORMANCE_REPORT, "URL_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT, "ADGROUP_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT, "CAMPAIGN_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.ACCOUNT_PERFORMANCE_REPORT, "ACCOUNT_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.GEO_PERFORMANCE_REPORT, "GEO_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.SEARCH_QUERY_PERFORMANCE_REPORT, "SEARCH_QUERY_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT, "MANAGED_PLAC_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT, "AUTOMATIC_PLAC_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_KEYWORDS_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_PLACEMENTS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_PLACEMENTS_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.AD_EXTENSIONS_PERFORMANCE_REPORT, "AD_EXTENSIONS_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.DESTINATION_URL_REPORT, "DEST_URL_REP"},
			{ADWORDS_V201302.ReportDefinitionReportType.CREATIVE_CONVERSION_REPORT, "CREATIVE_CONV_REP"},
			{ADWORDS_V201302.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT, "CRITERIA_PERF"},
			{ADWORDS_V201302.ReportDefinitionReportType.DISPLAY_TOPICS_PERFORMANCE_REPORT, "DISPLAY_TOPIC_PREF"},
			{ADWORDS_V201302.ReportDefinitionReportType.UNKNOWN, ""}
		};
	}
}
