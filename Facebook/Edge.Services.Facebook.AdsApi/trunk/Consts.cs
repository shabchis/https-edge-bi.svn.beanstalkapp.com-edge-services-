using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Facebook.AdsApi
{
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
			public const string adGroupTargeting = "AdGroupTargeting.xml";
			public const string Campaigns = "Campaigns.xml";
			public const string adGroup = "AdGroups.xml";
			public const string adGroupStats = "AdGroupStats.xml";
		}
	}
}
