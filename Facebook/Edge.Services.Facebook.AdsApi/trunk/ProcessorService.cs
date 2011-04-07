using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Readers;
using Edge.Data.Pipeline.GkManager;

namespace Edge.Services.Facebook.AdsApi
{
	public class ProcessorService : PipelineService
	{
		

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			//Campaigns
			DeliveryFile campaigns = this.Delivery.Files["GetCampaigns"];

			var campaignsReader = new XmlChunkReader
			(campaigns.SavedPath,
			Instance.Configuration.Options["facebook.ads.getCampaigns.xpath"],
			XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
			);


			Dictionary<string, CampaignData> campaignsData = new Dictionary<string, CampaignData>();
			using (campaignsReader)
			{
				while (campaignsReader.Read())
				{
					CampaignData camp = new CampaignData()
					{
						CampaignName = campaignsReader.Current["name"],
						CampaignID = campaignsReader.Current["campaign_id"],
						AccountID = campaignsReader.Current["account_id"],
						Status = campaignsReader.Current["campaign_status"]
					};
					campaignsData.Add(camp.CampaignID, camp);
				}
			}

			//GetAdGroups
			DeliveryFile adGroups = this.Delivery.Files["GetAdGroups"];

			var adGroupsReader = new XmlChunkReader
			(adGroups.SavedPath,
			Instance.Configuration.Options["facebook.ads.GetAdGroups.xpath"],
			XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
			);

			Dictionary<string, string> adsToCampaigns = new Dictionary<string, string>();

			using (adGroupsReader)
			{
				while (adGroupsReader.Read())
				{
					string campaignID=adGroupsReader.Current["campaign_id"];
					string adID=adGroupsReader.Current["ad_id"];
					adsToCampaigns.Add(adID, campaignID);
				}
			}


			//GetAdGroupStats
			DeliveryFile adGroupStats = this.Delivery.Files["GetAdGroupStats"];
			Dictionary<string, AdData> ads = new Dictionary<string, AdData>();
			var adGroupStatsReader = new XmlChunkReader
				(adGroupStats.SavedPath,
				Instance.Configuration.Options["Facebook.Ads.GetAdGroupStats.xpath"],// ./Ads_getAdGroupCreatives_response/ads_creative
				XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
				);

			using (adGroupStatsReader)
			{
				while (adGroupStatsReader.Read())
				{
					AdData ad = new AdData()
					{
					adgroup_id=adGroupStatsReader.Current["id"],
					Clicks = Convert.ToInt64(adGroupStatsReader.Current["clicks"]),
					Cost = Convert.ToDouble(adGroupStatsReader.Current["spent"]),
					Actions = Convert.ToInt32(adGroupStatsReader.Current["actions"]),
					Impressions = Convert.ToInt64(adGroupStatsReader.Current["impressions"]),
					Social_clicks = Convert.ToInt64(adGroupStatsReader.Current["social_clicks"]),
					Social_Cost = Convert.ToDouble(adGroupStatsReader.Current["social_spent"]),
					Social_impressions = Convert.ToInt64(adGroupStatsReader.Current["social_impressions"])
					};
					ads.Add(ad.adgroup_id, ad);
					
				}
			}





			//AdGroupCreatives + insert data
			DeliveryFile adGroupCreatives = this.Delivery.Files["GetAdGroupCreatives"];

			var adGroupCreativesReader = new XmlChunkReader
				(adGroupCreatives.SavedPath,
				Instance.Configuration.Options["Facebook.Ads.AdGroupCreatives.xpath"],// ./Ads_getAdGroupCreatives_response/ads_creative
				XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
				);
			
			using (adGroupCreativesReader)
			{

				while (adGroupCreativesReader.Read())
				{
					AdData ad = ads[adGroupCreativesReader.Current["adgroup_id"]];
					string campaignID = adsToCampaigns[ad.adgroup_id];
					CampaignData campaign = campaignsData[campaignID];
					AdDataUnit data = CreateUnitFromFacebookData(ad, campaign, adGroupCreativesReader.Current);

					data.Save();
					
				}
			}
		

			





			return Core.Services.ServiceOutcome.Success;
		}

		private AdDataUnit CreateUnitFromFacebookData(AdData ad, CampaignData campaign, Chunk chunk)
		{
			AdDataUnit unit = new AdDataUnit();
			//campaign
			unit.Extra.Account_OriginalID = campaign.AccountID;
			unit.Extra.Campaign_OriginalID = campaign.CampaignID;
			unit.Extra.Campaign_Name = campaign.CampaignName;
			unit.Extra.Campaign_OriginalStatus = campaign.Status;
			//*dailybudget

			//adgroup
			unit.Extra.Adgroup_Name = ad.title;
			unit.Extra.Adgroup_OriginalID = ad.adgroup_id;
			//*adgroup_status


			//adgroupCreative

			unit.Extra.Creative_OriginalID = ad.creative_id;
			unit.Extra.Creative_Title = ad.title;
			unit.Extra.Creative_Desc1 = ad.body;
			//*imageHash
			unit.Extra.AdgroupCreative_DestUrl = ad.link_url;
			unit.Extra.AdgroupCreative_VisUrl = ad.link_url;
			//TODO: unit.Extra.Tracker_Value=EXTRACTURL

			unit.AccountID =Convert.ToInt32( Delivery.Parameters["accountID"]);
			//TODO : unit.channelid
			unit.Impressions			= ad.Impressions;
			unit.Clicks					= ad.Clicks;
			unit.Cost					= ad.Cost;



			unit.CampaignGK = GkManager.GetCampaignGK(
				Convert.ToInt32(this.Delivery.Parameters["AccountID"]),
				Delivery.ChannelID,
				unit.Extra.Campaign_Name,
				unit.Extra.Campaign_OriginalID
				);

			// adgroup
			unit.AdgroupGK = GkManager.GetAdgroupGK(
				Convert.ToInt32(this.Delivery.Parameters["AccountID"]),
				Delivery.ChannelID,
				unit.CampaignGK.Value,
				unit.Extra.Adgroup_Name,
				unit.Extra.Adgroup_OriginalID
				);

			// keyword
			unit.KeywordGK = GkManager.GetKeywordGK(
				Convert.ToInt32(this.Delivery.Parameters["AccountID"]),
				unit.Extra.Keyword_Text
				);


			unit.CreativeGK = GkManager.GetCreativeGK(
				Convert.ToInt32(this.Delivery.Parameters["AccountID"]),
				unit.Extra.Creative_Title,
				unit.Extra.Creative_Desc1, null
				);
			
			long identifier=0; //TODO: GET identifier
			unit.TrackerGK = GkManager.GetTrackerGK(
				Convert.ToInt32(this.Delivery.Parameters["AccountID"]),
				identifier
				);	

			// TODO: currency conversion data
			// data.CurrencyID = Currency.GetByCode(data.Extra.Currency_Code).ID;

			//..................
			// METRICS
			//GET GK
			return unit;
		}

		
	}
	internal class CampaignData
	{
		public string CampaignID;
		public string CampaignName;
		public string AccountID;
		public string Status;

	}
	internal class AdData
	{
		public string adgroup_id;
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
		public long Impressions;
		public long Clicks;
		public double Cost;
		public long Social_impressions;
		public long Social_clicks;
		public double Social_Cost;
		public int Actions;



	}
	

}
