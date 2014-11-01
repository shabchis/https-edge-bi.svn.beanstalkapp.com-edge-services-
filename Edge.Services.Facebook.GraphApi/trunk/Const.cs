using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Facebook.GraphApi
{
	public static class FacebookConfigurationOptions
	{
		
		public const string BaseServiceAddress = "Facebook.BaseServiceAdress";
		public const string Account_ID = "Facebook.Account.ID";
		public const string Account_Name = "Facebook.Account.Name";
		public const string Auth_AccessToken = "Facebook.Auth.AccessToken";
        public const string Auth_AppSecret = "Facebook.Auth.AppSecret";
		public const string Ads_XPath_GetAdGroups = "Facebook.Ads.XPath.GetAdGroups";
		public const string Ads_XPath_GetCampaigns = "Facebook.Ads.XPath.GetCampaigns";
		public const string Ads_XPath_GetAdGroupStats = "Facebook.Ads.XPath.GetAdGroupStats";
		public const string Ads_XPath_GetAdGroupTargeting = "Facebook.Ads.XPath.GetAdGroupTargeting";
		public const string Ads_XPath_GetAdGroupCreatives = "Facebook.Ads.XPath.GetAdGroupCreatives";
		public const string AdGroupCreativeFields = "Facebook.Fields.AdGroupCreative";
		public const string AdGroupFields = "Facebook.Fields.AdGroup";
        public const string CampaignFields = "Facebook.Fields.Campaign";
        public const string CampaignGroupsFields = "Facebook.Fields.CampaignGroups";
	}

	public class Consts
	{
		public static class FacebookMethodsNames
		{
			public const string GetAdGroupTargeting = "adgrouptargeting";
			public const string GetCampaignsAdSets = "adcampaigns";
            public const string GetCampaignsGroups = "adcampaign_groups";
			public const string GetAdGroups = "adgroups";
			public const string GetAdGroupCreatives = "adcreatives";
			public const string GetAdGroupStats = "adgroupstats";
            public const string GetConversionStats = "adgroupconversions";
            public const string GetReportStats = "reportstats";
		}
		public static class FacebookMethodsParams
		{
			public const string StartTime = "start_time";
			public const string EndTime = "end_time";
			public const string IncludeDeleted = "include_deleted";
            public const string AdgroupStatus = "adgroup_status";
            public const string CampaignStatus = "campaign_group_status";
			public const string StatsMode = "stats_mode";
			public const string Fields = "fields";
		}
		public static class DeliveryFilesNames
		{
            public const string ConversionsStats = "ConversionsStats-{0}.json";
			public const string AdGroupTargeting = "AdGroupTargeting-{0}.json";
			public const string Campaigns = "AdSets_Formally_Campaign-{0}.json";
            public const string CampaignGroups = "CampaignGroups-{0}.json";
			public const string AdGroup = "AdGroups-{0}.json";
            public const string AdGroupStats = "AdGroupStats-{0}.json";
            public const string AdReportStats = "AdReportStats-{0}.json";
			public const string Creatives = "AdGroupCreatives-{0}.json";
		}
		public  enum FileTypes
		{			
			AdSets,
            CampaignGroups,
			AdGroups,
			AdGroupStats,
            ConversionsStats,
			Creatives			
		}
		public  enum FileSubType
		{
			Length=0,
			Data=1,
            New=2,
		}
		public static class DeliveryFileParameters
		{
			public const string Url = "URL";
			public const string FileSubType = "FileSubType";
			public const string FileType = "FileType";
		}
	}
}
