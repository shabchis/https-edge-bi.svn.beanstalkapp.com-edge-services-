using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Readers;
using Edge.Data.Pipeline.GkManager;

namespace Edge.Services.Facebook.AdsApi
{
	public class ProcessorService:PipelineService
	{
		protected override Core.Services.ServiceOutcome DoWork()
		{
			//AdGroupCreatives
			DeliveryFile adGroupCreatives = this.Delivery.Files["GetAdGroupCreatives"];

			var adGroupCreativesReader = new XmlChunkReader
				(adGroupCreatives.SavedPath,
				Instance.Configuration.Options["Facebook.Ads.AdGroupCreatives.xpath"],// ./Ads_getAdGroupCreatives_response/ads_creative
				XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
				);
			Dictionary<long, AdData> ads = new Dictionary<long, AdData>();
			using (adGroupCreativesReader)
			{
				
				while (adGroupCreativesReader.Read())
				{
					AdData ad = new AdData()
					{
						adgroup_id =Convert.ToInt64( adGroupCreativesReader.Current["adgroup_id"]),
						body = adGroupCreativesReader.Current["body"],
						title = adGroupCreativesReader.Current["title"],
						creative_id = adGroupCreativesReader.Current["creative_id"],
						name = adGroupCreativesReader.Current["name"],
						typename = adGroupCreativesReader.Current["typename"],
						link_url = adGroupCreativesReader.Current["link_url"],
						preview_url = adGroupCreativesReader.Current["preview_url"],
						link_type = adGroupCreativesReader.Current["link_type"],
						image_hash = adGroupCreativesReader.Current["image_hash"],
						type = adGroupCreativesReader.Current["type"],
						image_url = adGroupCreativesReader.Current["image_url"]
					};
					ads.Add(ad.adgroup_id, ad);
				}
			}


			//GetAdGroupStats
			DeliveryFile adGroupStats = this.Delivery.Files["GetAdGroupStats"];

			var adGroupStatsReader = new XmlChunkReader
				(adGroupStats.SavedPath,
				Instance.Configuration.Options["Facebook.Ads.GetAdGroupStats.xpath"],// ./Ads_getAdGroupCreatives_response/ads_creative
				XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
				);

			using (adGroupStatsReader)
			{
				while (adGroupStatsReader.Read())
				{
					AdData ad=ads[Convert.ToInt64(adGroupStatsReader.Current["id"])];
					ad.Stats.Clicks = Convert.ToInt64(adGroupStatsReader.Current["clicks"]);
					ad.Stats.Cost = Convert.ToDouble(adGroupStatsReader.Current["spent"]);
					ad.Stats.Actions = Convert.ToInt32(adGroupStatsReader.Current["actions"]);
					ad.Stats.Impressions = Convert.ToInt64(adGroupStatsReader.Current["impressions"]);

					
				}
			}



			//Campaigns
			DeliveryFile campaigns = this.Delivery.Files["GetCampaigns"];

			var campaignsReader = new XmlChunkReader
			(campaigns.SavedPath,
			Instance.Configuration.Options["facebook.ads.getCampaigns.xpath"],
			XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
			);


			Dictionary<long, CampaignData> campaignsData = new Dictionary<long, CampaignData>();
			using (campaignsReader)
			{
				while (campaignsReader.Read())
				{
					CampaignData camp = new CampaignData()
					{
					Campaign=campaignsReader.Current[""],
					Campaign_GK = GkManager.GetCampaignGK(Instance.AccountID,6,"CAMGINNAME?","WHATISTHIS"),
					CampaignId=Convert.ToInt64( campaignsReader.Current[""])
					
					};
					campaignsData.Add(camp.CampaignId, camp);
				}
			}

			//GetAdGroups
			DeliveryFile adGroups = this.Delivery.Files["GetAdGroups"];

			var adGroupsReader = new XmlChunkReader
			(adGroups.SavedPath,
			Instance.Configuration.Options["facebook.ads.GetAdGroups.xpath"],
			XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
			);



			using (adGroupsReader)
			{
				while (adGroupsReader.Read())
				{
					CampaignData camp = campaignsData[Convert.ToInt64( adGroupsReader.Current["campaign_id"])];
					AdData adData = new AdData()
					{
						adgroup_id = adGroupsReader.Current["ad_id"],
						name = adGroupsReader.Current["name"]
					};
					camp.CampaignAds[adData.adgroup_id] = adData;
				}
			}


			


		

			return Core.Services.ServiceOutcome.Success;
		}
	}
	internal class CampaignData
	{
		public long CampaignId;
		public string Campaign;
		public GKey Campaign_GK;
		public Dictionary<long, AdData> CampaignAds;

	}
	internal class AdData
	{
		public long adgroup_id;
		public string creative_id;		
		public string body;
		public string title;
		public string image_hash;
		public string link_url;
		public string name;
		public string preview_url;
		public string typename;
		public string link_type;
		public string type;
		public string image_url;
		public AdStats Stats;
		
		

	}
	internal class AdStats
	{
		public long Impressions;
		public long Clicks;
		public double Cost;
		public long Social_impressions;
		public long Social_clicks;
		public double Social_Cost;
		public int Actions;
	}
	

}
