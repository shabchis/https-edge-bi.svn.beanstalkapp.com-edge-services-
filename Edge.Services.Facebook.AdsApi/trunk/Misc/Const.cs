using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Facebook.AdsApi
{
	public static class FacebookConfigurationOptions
	{
		public const string BaseServiceAddress = "Facebook.BaseServiceAdress";
		public const string Account_ID = "Facebook.Account.ID";
		public const string Account_Name = "Facebook.Account.Name";
		public const string Auth_SessionKey = "Facebook.Auth.SessionKey";
		public const string Auth_ApiKey = "Facebook.Auth.ApiKey";
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
			public const string GetAdGroupTargeting = "facebook.ads.getAdGroupTargeting";
			public const string GetCampaigns = "facebook.ads.getCampaigns";
			public const string GetAdGroups = "facebook.ads.getAdGroups";
			public const string GetAdGroupCreatives = "facebook.ads.getAdGroupCreatives";
			public const string GetAdGroupStats = "facebook.ads.getAdGroupStats";
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
