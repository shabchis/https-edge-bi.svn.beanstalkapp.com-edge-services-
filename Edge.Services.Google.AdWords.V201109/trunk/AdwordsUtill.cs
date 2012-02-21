using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GA = Google.Api.Ads.AdWords;
using Edge.Data.Pipeline;
using System.Web;
using Google.Api.Ads.Common.Util;
using System.Xml;
using Google.Api.Ads.Common.Lib;
using Google.Api.Ads.AdWords.v201109;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Core.Utilities;

namespace Edge.Services.Google.AdWords
{


	public static class AdwordsUtill
	{


		public const string ADHOC_REPORT_URL_FORMAT = "{0}/api/adwords/reportdownload/v201109";

		public static GA.v201109.ReportDefinition CreateNewReportDefinition(DeliveryFile deliveryFile, string startDate, string endDate)
		{
			//Create ReportDefintion
			GA.v201109.ReportDefinition definition = new GA.v201109.ReportDefinition();

			if (Enum.IsDefined(typeof(GA.v201109.ReportDefinitionReportType), deliveryFile.Parameters["ReportType"].ToString()))
				definition.reportType = (GA.v201109.ReportDefinitionReportType)Enum.Parse(typeof(GA.v201109.ReportDefinitionReportType), deliveryFile.Parameters["ReportType"].ToString(), true);

			definition.reportName = deliveryFile.Name;
			definition.downloadFormat = GA.v201109.DownloadFormat.GZIPPED_CSV;
			definition.dateRangeType = GA.v201109.ReportDefinitionDateRangeType.CUSTOM_DATE;

			// Create the selector.
			/*----------------------------------------------------------------*/
			GA.v201109.Selector selector = GetNewSelector(startDate, endDate, definition.reportType, deliveryFile.Parameters["ReportFieldsType"].ToString());


			definition.selector = selector;
			return definition;
		}

		public static GA.v201109.Selector GetNewSelector(string startDate, string endDate, GA.v201109.ReportDefinitionReportType reportType, string reportFieldsType)
		{
			GA.v201109.Selector selector = new GA.v201109.Selector();

			//Setting report fields
			selector.fields = GoogleStaticReportFields.ReportNames[reportType][reportFieldsType];

			//Setting Date Range
			selector.dateRange = new GA.v201109.DateRange
			{
				min = startDate,
				max = endDate
			};

			if (selector.fields.Contains("Impressions"))
			{
				//Setting Imps Fillter
				GA.v201109.Predicate impPredicate = new GA.v201109.Predicate();
				impPredicate.field = "Impressions";
				impPredicate.@operator = GA.v201109.PredicateOperator.GREATER_THAN;
				impPredicate.values = new string[] { "0" };
				selector.predicates = new GA.v201109.Predicate[] { impPredicate };
				/*----------------------------------------------------------------*/
			}
			return selector;

		}

		public static string ConvertDefinitionToXml(GA.v201109.ReportDefinition definition)
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
		public static Dictionary<GA.v201109.ReportDefinitionReportType, string> _reportNames = new Dictionary<GA.v201109.ReportDefinitionReportType, string>()
		{
			{GA.v201109.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT, "KEYWORDS_PERF"},
			{GA.v201109.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, "AD_PERF"},
			{GA.v201109.ReportDefinitionReportType.URL_PERFORMANCE_REPORT, "URL_PERF"},
			{GA.v201109.ReportDefinitionReportType.ADGROUP_PERFORMANCE_REPORT, "ADGROUP_PERF"},
			{GA.v201109.ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT, "CAMPAIGN_PERF"},
			{GA.v201109.ReportDefinitionReportType.ACCOUNT_PERFORMANCE_REPORT, "ACCOUNT_PERF"},
			{GA.v201109.ReportDefinitionReportType.DEMOGRAPHIC_PERFORMANCE_REPORT, "DEMOGRAPHIC_PERF"},
			{GA.v201109.ReportDefinitionReportType.GEO_PERFORMANCE_REPORT, "GEO_PERF"},
			{GA.v201109.ReportDefinitionReportType.SEARCH_QUERY_PERFORMANCE_REPORT, "SEARCH_QUERY_PERF"},
			{GA.v201109.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT, "MANAGED_PLAC_PERF"},
			{GA.v201109.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT, "AUTOMATIC_PLAC_PERF"},
			{GA.v201109.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_KEYWORDS_PERF"},
			{GA.v201109.ReportDefinitionReportType.CAMPAIGN_NEGATIVE_PLACEMENTS_PERFORMANCE_REPORT, "CAMPAIGN_NEG_PLACEMENTS_PERF"},
			{GA.v201109.ReportDefinitionReportType.AD_EXTENSIONS_PERFORMANCE_REPORT, "AD_EXTENSIONS_PERF"},
			{GA.v201109.ReportDefinitionReportType.DESTINATION_URL_REPORT, "DEST_URL_REP"},
			{GA.v201109.ReportDefinitionReportType.CREATIVE_CONVERSION_REPORT, "CREATIVE_CONV_REP"},
			{GA.v201109.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT, "CRITERIA_PERF"},
			{GA.v201109.ReportDefinitionReportType.UNKNOWN, ""}
		};

	}

	public static class ReportDefinitionReportFieldsType
	{
		public static string DEFAULT = "DEFAULT";
		public static string CONVERSION = "CONVERSION";
	}

	public static class GoogleStaticReportFields
	{

		#region Supported Reports fields

		static string[] AD_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "AdGroupName", "AdGroupStatus", "CampaignId", "CampaignName", "Impressions","Clicks", "Cost","Headline",
		                                                   "Description1","Description2", "KeywordId", "DisplayUrl","CreativeDestinationUrl","CampaignStatus","AccountTimeZoneId",
		                                                   "AdType","AccountCurrencyCode","Ctr","Status","AveragePosition","Conversions",
		                                                   "ConversionRate","ConversionRateManyPerClick","ConversionSignificance",
		                                                   "ConversionsManyPerClick",
		                                                   "ConversionValue","TotalConvValue","ValuePerConversion","ValuePerConversionManyPerClick","ValuePerConvManyPerClick","ViewThroughConversions","ViewThroughConversionsSignificance",
		                                                   "AdNetworkType1"
		                                               };

		static string[] AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = { "Id", "KeywordId", "ConversionsManyPerClick", "ConversionCategoryName" };

		static string[] KEYWORDS_PERFORMANCE_REPORT_FIELDS = { "Id", "AdGroupId", "CampaignId", "KeywordText", "KeywordMatchType", "Impressions", "Clicks", "Cost", "Status", "DestinationUrl", "QualityScore" };

		static string[] DESTINATION_URL_REPORT = { "AdGroupName","CampaignName","EffectiveDestinationUrl", "Impressions", "Clicks", "Cost", "ValuePerConv", "ValuePerConversion",
												   "ValuePerConversionManyPerClick", "ValuePerConvManyPerClick","ViewThroughConversions","AverageCpc","AveragePosition"};

		static string[] MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS = { "Id", "CampaignId", "AdGroupId", "DestinationUrl", "PlacementUrl", "Status" };

		static string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS = {"Clicks", "Cost", "Impressions", "CampaignId","CampaignName", "AdGroupId","AdGroupName",
																			 "CriteriaParameters", "Domain","AdNetworkType1","AdNetworkType2","AverageCpc",
																			 "AverageCpm","ConversionsManyPerClick","Conversions","CostPerConversion","Ctr","IsAutoOptimized",
																			 "TotalConvValue","ViewThroughConversions","ConversionValue"
																		 };
		static string[] AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION = {"ConversionsManyPerClick", "ConversionCategoryName" };
		static string[] CRITERIA_PERFORMANCE_REPORT_FIELDS = { "id","CriteriaDestinationUrl","ConversionsManyPerClick", "ConversionCategoryName" };
																		 


		#endregion Reports fields

		public static Dictionary<GA.v201109.ReportDefinitionReportType, Dictionary<string, string[]>> ReportNames = new Dictionary<GA.v201109.ReportDefinitionReportType, Dictionary<string, string[]>>()
		{
			{GA.v201109.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT,
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,KEYWORDS_PERFORMANCE_REPORT_FIELDS}}
			},
			{GA.v201109.ReportDefinitionReportType.AD_PERFORMANCE_REPORT, 
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AD_PERFORMANCE_REPORT_FIELDS} , 
													{ReportDefinitionReportFieldsType.CONVERSION,AD_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION}}
			},
			{GA.v201109.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT, 
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,MANAGED_PLACEMENTS_PERFORMANCE_REPORT_FIELDS}}
			},
			{GA.v201109.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.CONVERSION,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION	}
				}
			},
			{GA.v201109.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT,  
				new Dictionary<string, string[]>(){ {ReportDefinitionReportFieldsType.DEFAULT,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS},
													{ReportDefinitionReportFieldsType.CONVERSION,AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS_WITH_CONVERSION	}
				}
			}
		};
	}
}
