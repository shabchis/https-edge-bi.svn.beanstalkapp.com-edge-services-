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
		public const string Auth_SessionKey = "Facebook.Auth.SessionKey";
		public const string Auth_ApiKey = "Facebook.Auth.ApiKey";
		public const string Auth_RedirectUri = "Facebook.Auth.RedirectUri";
		public const string Auth_Permision = "Facebook.Auth.RedirectUri";
		public const string Auth_AuthenticationUrl = "Facebook.AuthenticationUrl";
		public const string Auth_AppSecret = "Facebook.Auth.AppSecret";
		public const string Auth_SessionSecret = "Facebook.Auth.SessionSecret";
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
			public const string EndTime = "start_time";


			public static string IncludeDeleted = "include_deleted";
		}
		public static class DeliveryFilesNames
		{
			public const string AdGroupTargeting = "AdGroupTargeting.xml";
			public const string Campaigns = "Campaigns.xml";
			public const string AdGroup = "AdGroups.xml";
			public const string AdGroupStats = "AdGroupStats.xml";
			public const string Creatives = "AdGroupCreatives-{0}.xml";
		}
		public static class DeliveryFileParameters
		{
			public const string Body = "body";
		}
	}
}
