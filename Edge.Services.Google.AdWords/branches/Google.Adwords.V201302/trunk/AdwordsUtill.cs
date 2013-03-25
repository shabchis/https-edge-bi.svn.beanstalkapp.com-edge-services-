using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GA = Google.Api.Ads.AdWords;
using Edge.Data.Pipeline;
using System.Web;
using System.Xml;
using ADWORDS_V201302 = Google.Api.Ads.AdWords.v201302;
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


		public const string ADHOC_REPORT_URL_FORMAT = "{0}/api/adwords/reportdownload/v201302";

		public static ADWORDS_V201302.ReportDefinition CreateNewReportDefinition(DeliveryFile deliveryFile, string startDate, string endDate, bool filterOnImps = false, bool filterDeleted = false)
		{
			//Create ReportDefintion
			ADWORDS_V201302.ReportDefinition definition = new ADWORDS_V201302.ReportDefinition();

			if (Enum.IsDefined(typeof(ADWORDS_V201302.ReportDefinitionReportType), deliveryFile.Parameters["ReportType"].ToString()))
				definition.reportType = (ADWORDS_V201302.ReportDefinitionReportType)Enum.Parse(typeof(ADWORDS_V201302.ReportDefinitionReportType), deliveryFile.Parameters["ReportType"].ToString(), true);

			definition.reportName = deliveryFile.Name;
			definition.downloadFormat = ADWORDS_V201302.DownloadFormat.GZIPPED_CSV;
			definition.dateRangeType = ADWORDS_V201302.ReportDefinitionDateRangeType.CUSTOM_DATE;

			// Create the selector.
			/*----------------------------------------------------------------*/
			ADWORDS_V201302.Selector selector = GetNewSelector(startDate, endDate, definition.reportType, deliveryFile.Parameters["ReportFieldsType"].ToString(), filterOnImps, filterDeleted);


			definition.selector = selector;
			return definition;
		}

		public static ADWORDS_V201302.Selector GetNewSelector(string startDate, string endDate, ADWORDS_V201302.ReportDefinitionReportType reportType, string reportFieldsType, bool filterOnImps, bool filterDeleted)
		{
			ADWORDS_V201302.Selector selector = new ADWORDS_V201302.Selector();

			//Setting report fields
			selector.fields = GoogleStaticReportFields.ReportNames[reportType][reportFieldsType];

			//Setting Date Range
			selector.dateRange = new ADWORDS_V201302.DateRange
			{
				min = startDate,
				max = endDate
			};
			#region predicates
			/****************************************************************/
			List<ADWORDS_V201302.Predicate> predicate = new List<ADWORDS_V201302.Predicate>();

			if (filterDeleted)
			{
				//Set deleted campigns filter
				List<string> deletedCampaigns = GetDeletedCampaigns();
				if (deletedCampaigns.Count > 0)
				{
					ADWORDS_V201302.Predicate camPredicate = new ADWORDS_V201302.Predicate();
					camPredicate.field = "CampaignId";
					camPredicate.@operator = ADWORDS_V201302.PredicateOperator.NOT_IN;
					camPredicate.values = deletedCampaigns.ToArray<string>();
					predicate.Add(camPredicate);
				}

				//Set deleted adgroups filter
				List<string> deletedAdgroup = GetDeletedAdgroups();
				if (deletedAdgroup.Count > 0)
				{
					//Set deleted campigns filter
					ADWORDS_V201302.Predicate AgPredicate = new ADWORDS_V201302.Predicate();
					AgPredicate.field = "AdGroupId";
					AgPredicate.@operator = ADWORDS_V201302.PredicateOperator.NOT_IN;
					AgPredicate.values = deletedCampaigns.ToArray<string>();
					predicate.Add(AgPredicate);
				}
			}


			if (filterOnImps && selector.fields.Contains("Impressions"))
			{
				//Set Imps Fillter
				ADWORDS_V201302.Predicate impPredicate = new ADWORDS_V201302.Predicate();
				impPredicate.field = "Impressions";
				impPredicate.@operator = ADWORDS_V201302.PredicateOperator.GREATER_THAN;
				impPredicate.values = new string[] { "0" };
				/*----------------------------------------------------------------*/

				//Set clicks Fillter
				//ADWORDS_V201302.Predicate clicksPredicate = new ADWORDS_V201302.Predicate();
				//clicksPredicate.field = "Clicks";
				//clicksPredicate.@operator = ADWORDS_V201302.PredicateOperator.NOT_EQUALS;
				//clicksPredicate.values = new string[] { "0" };

				///*----------------------------------------------------------------*/

				////Set clicks Fillter
				//ADWORDS_V201302.Predicate costPredicate = new ADWORDS_V201302.Predicate();
				//costPredicate.field = "Cost";
				//costPredicate.@operator = ADWORDS_V201302.PredicateOperator.NOT_EQUALS;
				//costPredicate.values = new string[] { "0" };

				/*----------------------------------------------------------------*/

				predicate.Add(impPredicate);

			}
			selector.predicates = predicate.ToArray<ADWORDS_V201302.Predicate>();
			/****************************************************************/

			#endregion


			return selector;

		}

		public static List<string> GetDeletedCampaigns()
		{
			List<string> deletedCampaigns = new List<string>();

			SqlConnection connection = new SqlConnection(AppSettings.GetConnectionString(typeof(AdwordsUtill), "systemDB"));

			try
			{
				using (connection)
				{
					string cmdTxt = " SELECT [campaignid] from [dbo].[UserProcess_GUI_PaidCampaign] WHERE [campStatus] = @status";
					SqlCommand cmd = new SqlCommand(cmdTxt);
					cmd.Connection = connection;
					connection.Open();
					SqlParameter status = new SqlParameter("@status", Convert.ToInt32(ObjectStatus.Deleted));
					cmd.Parameters.Add(status);

					using (SqlDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							deletedCampaigns.Add(reader[0].ToString());
						}
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error while trying to get deleted campigns from table UserProcess_GUI_PaidCampaign", ex);
			}

			return deletedCampaigns;
		}

		public static List<string> GetDeletedAdgroups()
		{
			List<string> deletedAdgroups = new List<string>();

			SqlConnection connection = new SqlConnection(AppSettings.GetConnectionString(typeof(AdwordsUtill), "systemDB"));

			try
			{
				using (connection)
				{
					string cmdTxt = "SELECT [adgroupID] from [dbo].[UserProcess_GUI_PaidAdGroup] WHERE [agStatus] = @status";
					SqlCommand cmd = DataManager.CreateCommand(cmdTxt);
					cmd.Connection = connection;
					connection.Open();
					SqlParameter status = new SqlParameter("@status", Convert.ToInt32(ObjectStatus.Deleted));
					cmd.Parameters.Add(status);

					cmd.Parameters["@status"].Value = ObjectStatus.Deleted.ToString();

					using (SqlDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							deletedAdgroups.Add(reader[0].ToString());
						}
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error while trying to get deleted Adgroups from table UserProcess_GUI_PaidAdGroup", ex);
			}
			return deletedAdgroups;
		}

		public static string ConvertDefinitionToXml(ADWORDS_V201302.ReportDefinition definition)
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
		public static Dictionary<ADWORDS_V201302.ReportDefinitionReportType, string> _reportNames = new Dictionary<ADWORDS_V201302.ReportDefinitionReportType, string>()
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

	public static class ReportDefinitionReportFieldsType
	{
		public static string DEFAULT = "DEFAULT";
		public static string CONVERSION = "CONVERSION";
		public static string STATUS = "STATUS";
	}

	public static class GoogleStaticReportFields
	{

		#region Supported Reports fields

		public static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions","Clicks", "Cost","Headline",
		                                                   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl","CampaignStatus","AccountTimeZoneId",
		                                                   "AdType","AccountCurrencyCode","Ctr","Status","AveragePosition","Conversions",
		                                                   "ConversionRate","ConversionRateManyPerClick","ConversionsManyPerClick","ConversionValue","TotalConvValue"
		                                                  
		                                               };
		public static string[] AD_PERFORMANCE_REPORT_FIELDS_STATUS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName","CampaignStatus",
		                                                   "Status"
		                                               };
		public static string[] AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "Id", "KeywordId", "ConversionsManyPerClick", "ConversionCategoryName" };

		public static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "CampaignId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost", "Status", "DestinationUrl", "QualityScore" };
		public static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS_STATUS = { "Id", "AdGroupId", "CampaignId", "Status" };

		public static string[] DESTINATION_URL_REPORT = { "AdGroupName","CampaignName","EffectiveDestinationUrl", "Impressions", "Clicks", "Cost", "ValuePerConv", "ValuePerConversion",
												   "ValuePerConversionManyPerClick", "ValuePerConvManyPerClick","ViewThroughConversions","AverageCpc","AveragePosition"};

		public static string[] MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS = { "Id", "CampaignId", "AdGroupId", "DestinationUrl", "PlacementUrl", "Status" };
		public static string[] MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_STATUS = { "Id", "CampaignId", "AdGroupId", "Status" };

		public static string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS = { "CampaignId","CampaignName","CampaignStatus", "AdGroupId","AdGroupName","Clicks", "Cost", "Impressions",
																			 "Domain","ConversionsManyPerClick","Conversions"
																		 };
		public static string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "ConversionsManyPerClick", "ConversionCategoryName" };
		public static string[] CRITERIA_PERFORMANCE_REPORT_FIELDS = { "id", "CriteriaDestinationUrl", "ConversionsManyPerClick", "ConversionCategoryName" };

		public static string[] Display_Topics_Performance_Report = { "AdGroupId", "AdGroupName", "CampaignId", "CampaignName", "Clicks", "Cost", "CpcBidSource", "Criteria", "CriteriaDestinationUrl", "Date", "DestinationUrl", "Id", "Impressions", "IsNegative", "MaxCpc", "MaxCpm" };


		#endregion Reports fields

		public static Dictionary<ADWORDS_V201302.ReportDefinitionReportType, Dictionary<string, string[]>> ReportNames = new Dictionary<ADWORDS_V201302.ReportDefinitionReportType, Dictionary<string, string[]>>()
		{
			
			{ADWORDS_V201302.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT,
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,KEYWORDS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.STATUS,KEYWORDS_PERFORMANCE_REPORT_FIELDS_STATUS}
												  }
			},
			{ADWORDS_V201302.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, 
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AD_PERFORMANCE_REPORT_FIELDS} , 
													{ReportDefinitionReportFieldsType.CONVERSION,AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION},
													{ReportDefinitionReportFieldsType.STATUS,AD_PERFORMANCE_REPORT_FIELDS_STATUS},
												  }
			},
			{ADWORDS_V201302.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT, 
				new Dictionary<string, string[]>(){ 
													{ReportDefinitionReportFieldsType.DEFAULT,MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.STATUS,MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_STATUS},
												  }
			},
			{ADWORDS_V201302.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.CONVERSION,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION	}
				}
			},
			{ADWORDS_V201302.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.CONVERSION,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION	}
				}
			},
			{ADWORDS_V201302.ReportDefinitionReportType.DISPLAY_TOPICS_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,Display_Topics_Performance_Report}
												
				}
			}
		};
	}
}
