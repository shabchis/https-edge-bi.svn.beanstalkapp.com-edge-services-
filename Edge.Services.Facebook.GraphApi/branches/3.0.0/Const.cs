namespace Edge.Services.Facebook.GraphApi
{
	public static class FacebookConfigurationOptions
	{
		public const string BaseServiceAddress = "Facebook.BaseServiceAdress";
		public const string Account_ID = "Facebook.Account.ID";
		public const string Account_Name = "Facebook.Account.Name";
		public const string AccessToken = "Facebook.AccessToken";
		public const string Ads_XPath_GetAdGroups = "Facebook.Ads.XPath.GetAdGroups";
		public const string Ads_XPath_GetCampaigns = "Facebook.Ads.XPath.GetCampaigns";
		public const string Ads_XPath_GetAdGroupStats = "Facebook.Ads.XPath.GetAdGroupStats";
		public const string Ads_XPath_GetAdGroupTargeting = "Facebook.Ads.XPath.GetAdGroupTargeting";
		public const string Ads_XPath_GetAdGroupCreatives = "Facebook.Ads.XPath.GetAdGroupCreatives";
	}

	public class Consts
	{
		public static class FacebookMethodsNames
		{
			public const string GetAdGroupTargeting = "adgrouptargeting";
			public const string GetCampaigns = "adcampaigns";
			public const string GetAdGroups = "adgroups";
			public const string GetAdGroupCreatives = "adcreatives";
			public const string GetAdGroupStats = "adgroupstats";
		}
		public static class FacebookMethodsParams
		{
			public const string StartTime = "start_time";
			public const string EndTime = "end_time";
			public const string IncludeDeleted = "include_deleted";
			public const string StatsMode = "stats_mode";
		}
		public static class DeliveryFilesNames
		{
			public const string AdGroupTargeting = "AdGroupTargeting-{0}.json";
			public const string Campaigns = "Campaigns-{0}.json";
			public const string AdGroup = "AdGroups-{0}.json";
			public const string AdGroupStats = "AdGroupStats-{0}.json";
			public const string Creatives = "AdGroupCreatives-{0}.json";
		}
		public enum FileTypes
		{
			Campaigns,
			AdGroups,
			AdGroupStats,
			Creatives
		}
		public enum FileSubType
		{
			Length = 0,
			Data = 1,
		}
		public static class DeliveryFileParameters
		{
			public const string Url = "URL";
			public const string FileSubType = "FileSubType";
			public const string FileType = "FileType";
		}
	}
}
