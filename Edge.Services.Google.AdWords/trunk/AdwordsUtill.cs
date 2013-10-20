using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GA = Google.Api.Ads.AdWords;
using Edge.Data.Pipeline;
using System.Web;
using System.Xml;
using ADWORDS = Google.Api.Ads.AdWords.v201309;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Core.Utilities;
using Edge.Data.Objects;
using Google.Api.Ads.Common.Util;
using Google.Api.Ads.Common.Lib;

namespace Edge.Services.Google.AdWords
{


	public static class AdwordsUtill
	{
		public static string GetAuthToken(GA.Lib.AdWordsUser user, bool generateNew = false)
		{

			string pass;
			string auth = string.Empty;

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
				Log.Write("Error while trying to create new Auth key", ex);
				throw new Exception("Error while trying to create new Auth key", ex);
			}
			return auth;

		}

		private static void SaveAuthTokenToDB(string mccEmail, string authToken)
		{
			SqlConnection connection;

			connection = new SqlConnection(AppSettings.GetConnectionString(typeof(AdwordsUtill), "MCC_Auth"));

			try
			{
				using (connection)
				{
					SqlCommand cmd = DataManager.CreateCommand(@"SetGoogleMccAuth(@MccEmail:Nvarchar,@AuthToken:Nvarchar)", System.Data.CommandType.StoredProcedure);
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
			string auth = "";
			mccPassword = "";
			SqlConnection connection;

			connection = new SqlConnection(AppSettings.GetConnectionString(typeof(AdwordsUtill), "MCC_Auth"));
			try
			{
				using (connection)
				{
					SqlCommand cmd = DataManager.CreateCommand(@"GetGoogleMccAuth(@MccEmail:Nvarchar)", System.Data.CommandType.StoredProcedure);
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
		public static Dictionary<ADWORDS.ReportDefinitionReportType, string> _reportNames = new Dictionary<ADWORDS.ReportDefinitionReportType, string>()
		{
			{ADWORDS.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT, "KEYWORDS_PERF"},
			{ADWORDS.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, "AD_PERF"},
			{ADWORDS.ReportDefinitionReportType.URL_PERFORMANCE_REPORT, "URL_PERF"},
			{ADWORDS.ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT, "ADGROUP_PERF"},
			{ADWORDS.ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT, "CAMPAIGN_PERF"},
			{ADWORDS.ReportDefinitionReportType.ACCOUNT_PERFORMANCE_REPORT, "ACCOUNT_PERF"},
			{ADWORDS.ReportDefinitionReportType.GEO_PERFORMANCE_REPORT, "GEO_PERF"},
			{ADWORDS.ReportDefinitionReportType.SEARCH_QUERY_PERFORMANCE_REPORT, "SEARCH_QUERY_PERF"},
			{ADWORDS.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT, "MANAGED_PLAC_PERF"},
			{ADWORDS.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT, "AUTOMATIC_PLAC_PERF"},
			{ADWORDS.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_KEYWORDS_PERF"},
			{ADWORDS.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_PLACEMENTS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_PLACEMENTS_PERF"},
			{ADWORDS.ReportDefinitionReportType.AD_EXTENSIONS_PERFORMANCE_REPORT, "AD_EXTENSIONS_PERF"},
			{ADWORDS.ReportDefinitionReportType.DESTINATION_URL_REPORT, "DEST_URL_REP"},
			{ADWORDS.ReportDefinitionReportType.CREATIVE_CONVERSION_REPORT, "CREATIVE_CONV_REP"},
			{ADWORDS.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT, "CRITERIA_PERF"},
			{ADWORDS.ReportDefinitionReportType.DISPLAY_TOPICS_PERFORMANCE_REPORT, "DISPLAY_TOPIC_PREF"},
			{ADWORDS.ReportDefinitionReportType.PLACEHOLDER_FEED_ITEM_REPORT, "PLACEHOLDER_REPORT"},
			{ADWORDS.ReportDefinitionReportType.UNKNOWN, ""}
		};

	}

	public static class ReportDefinitionReportFieldsType
	{
		public static string DEFAULT = "DEFAULT";
		public static string CONVERSION = "CONVERSION";
		public static string STATUS = "STATUS";
	}

	public static class GoogleStaticReportFields
	{

		#region Supported Reports fields

        private static string[] PLACEHOLDER_FEED_ITEM_REPORT = {"FeedItemId","FeedId","DevicePreference","Cost","ClickType","Clicks","CampaignName","CampaignId","AdGroupId","AdGroupName","AttributeValues"};

		private static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions","Clicks", "Cost","Headline",
		                                                   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl","CampaignStatus","AccountTimeZoneId",
		                                                   "AdType","AccountCurrencyCode","Ctr","Status","AveragePosition","Conversions","DevicePreference",
		                                                   "ConversionRate","ConversionRateManyPerClick","ConversionsManyPerClick","ConversionValue","TotalConvValue"
		                                                  
		                                               };
        private static string[] AD_PERFORMANCE_REPORT_FIELDS_STATUS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName","CampaignStatus",
		                                                   "Status"
		                                               };
        private static string[] AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "Id", "KeywordId", "ConversionsManyPerClick", "ConversionCategoryName" };

        private static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "CampaignId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost", "Status", "DestinationUrl", "QualityScore" };
        private static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS_STATUS = { "Id", "AdGroupId", "CampaignId", "Status" };

        private static string[] DESTINATION_URL_REPORT = { "AdGroupName","CampaignName","EffectiveDestinationUrl", "Impressions", "Clicks", "Cost", "ValuePerConv", "ValuePerConversion",
												   "ValuePerConversionManyPerClick", "ValuePerConvManyPerClick","ViewThroughConversions","AverageCpc","AveragePosition"};

        private static string[] MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS = { "Id", "CampaignId", "AdGroupId", "DestinationUrl", "PlacementUrl", "Status", "Impressions", "Clicks", "Cost" };
        private static string[] MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_STATUS = { "Id", "CampaignId", "AdGroupId", "Status" };

        private static string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS = { "CampaignId","CampaignName","CampaignStatus", "AdGroupId","AdGroupName","Clicks", "Cost", "Impressions",
																			 "Domain","ConversionsManyPerClick","Conversions"
																		 };
        private static string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "ConversionsManyPerClick", "ConversionCategoryName" };
        private static string[] CRITERIA_PERFORMANCE_REPORT_FIELDS = { "id", "CriteriaDestinationUrl", "ConversionsManyPerClick", "ConversionCategoryName" };

        private static string[] Display_Topics_Performance_Report = { "AdGroupId", "AdGroupName", "CampaignId", "CampaignName", "Clicks", "Cost", "CpcBidSource", "Criteria", "CriteriaDestinationUrl", "Date", "DestinationUrl", "Id", "Impressions", "IsNegative", "MaxCpc", "MaxCpm" };


		#endregion Reports fields

		public static Dictionary<ADWORDS.ReportDefinitionReportType, Dictionary<string, string[]>> REPORTS_FIELDS = new Dictionary<ADWORDS.ReportDefinitionReportType, Dictionary<string, string[]>>()
		{
			
			{ADWORDS.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT,
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,KEYWORDS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.STATUS,KEYWORDS_PERFORMANCE_REPORT_FIELDS_STATUS}
												  }
			},
			{ADWORDS.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, 
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AD_PERFORMANCE_REPORT_FIELDS} , 
													{ReportDefinitionReportFieldsType.CONVERSION,AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION},
													{ReportDefinitionReportFieldsType.STATUS,AD_PERFORMANCE_REPORT_FIELDS_STATUS},
												  }
			},
			{ADWORDS.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT, 
				new Dictionary<string, string[]>(){ 
													{ReportDefinitionReportFieldsType.DEFAULT,MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.STATUS,MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_STATUS},
												  }
			},
			{ADWORDS.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.CONVERSION,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION	}
				}
			},
			{ADWORDS.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.CONVERSION,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION	}
				}
			},
			{ADWORDS.ReportDefinitionReportType.DISPLAY_TOPICS_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,Display_Topics_Performance_Report}
												
				}
			},
            {ADWORDS.ReportDefinitionReportType.PLACEHOLDER_FEED_ITEM_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,PLACEHOLDER_FEED_ITEM_REPORT}
												
				}
			}
		};
	}
}
