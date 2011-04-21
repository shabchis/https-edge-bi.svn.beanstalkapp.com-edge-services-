using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Readers;
//using Edge.Data.Pipeline.GkManager;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Objects;
using Edge.Data.Pipeline.Deliveries;

namespace Edge.Services.Facebook.AdsApi
{


	/*

	* step 1:
	 * (Campaigns) V
	 * 
	 * step 2:
	 * - Ad.Guid = Guid.NewGuid()-----cancelded
	 * - Ad.OriginalID
	 * - Ad.Campaign
	 * 
	 * step 3:
	 * >>> AdMetricsUnit.Ad.Guid + Impressions, clicks, etc.
	 * 
	 * 
	 * 
	 * step 4:
	 * - Ad.targeting
	 * 
	* step 5:
	 * >>>> Ad.creatives + remove from dictionary



	*/

	public class ProcessorService : PipelineService
	{


		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			//Campaigns
			DeliveryFile campaigns = this.Delivery.Files["GetCampaigns"];

			var campaignsReader = new XmlDynamicReader
			(campaigns.FileInfo.Location.ToString(), Instance.Configuration.Options["facebook.ads.getCampaigns.xpath"]);
			Dictionary<string, Campaign> campaignsData = new Dictionary<string, Campaign>();
			using (campaignsReader)
			{
				while (campaignsReader.Read())
				{
					Campaign camp = new Campaign()
					{
						Name = campaignsReader.Current.name,
						OriginalID = campaignsReader.Current.campaign_id,
						Status = campaignsReader.Current.campaign_status,
						Channel = new Channel()
						{
							ID = Delivery.Channel.ID
						},
						Account = new Account()
						{
							ID = Instance.AccountID
						}

					};
					campaignsData.Add(camp.OriginalID, camp);
				}
				campaigns.History.Add(DeliveryOperation.Processed, Instance.InstanceID);
			}

			//GetAdGroups
			DeliveryFile adGroups = this.Delivery.Files["GetAdGroups"];

			var adGroupsReader = new XmlDynamicReader
			(adGroups.FileInfo.Location.ToString(),
			Instance.Configuration.Options["facebook.ads.GetAdGroups.xpath"]);


			Dictionary<string, Ad> ads = new Dictionary<string, Ad>();
			using (adGroupsReader)
			{
				while (adGroupsReader.Read())
				{
					Ad ad = new Ad()
					{
						OriginalID = adGroupsReader.Current.ad_id,
						Campaign = campaignsData[adGroupsReader.Current.campaign_id]

					};
					ads.Add(ad.OriginalID, ad);
					
				}
				adGroups.History.Add(DeliveryOperation.Processed, Instance.InstanceID);
			}


			//GetAdGroupStats
			DeliveryFile adGroupStats = this.Delivery.Files["GetAdGroupStats"];
			
			var adGroupStatsReader = new XmlDynamicReader
				(adGroupStats.FileInfo.Location.ToString(), Instance.Configuration.Options["Facebook.Ads.GetAdGroupStats.xpath"]);// ./Ads_getAdGroupCreatives_response/ads_creative


			using (var session = new AdDataImportSession(this.Delivery))
			{
				using (adGroupStatsReader)
				{
					while (adGroupStatsReader.Read())
					{
						
						
						AdMetricsUnit adMetricsUnit = new AdMetricsUnit()
						{
							Ad=ads[adGroupStatsReader.Current.id],
							Clicks = Convert.ToInt64(adGroupStatsReader.Current.clicks),
							Cost = Convert.ToDouble(adGroupStatsReader.Current.spent),
							Impressions = Convert.ToInt64(adGroupStatsReader.Current.impressions),
							//Actions = Convert.ToInt32(adGroupStatsReader.Current.actions),						
							//Social_clicks = Convert.ToInt64(adGroupStatsReader.Current.social_clicks),
							//Social_Cost = Convert.ToDouble(adGroupStatsReader.Current.social_spent),
							//Social_impressions = Convert.ToInt64(adGroupStatsReader.Current.social_impressions)						

						};
						session.ImportMetrics(adMetricsUnit);

					}
					adGroupStats.History.Add(DeliveryOperation.Processed, Instance.InstanceID); //TODO: HISTORY WHEN?PROCCESED IS AFTER DATABASE'?
				}


				//getAdGroupTargeting

				DeliveryFile adGroupTargeting = this.Delivery.Files["getAdGroupTargeting"];
				var adGroupTargetingReader = new XmlDynamicReader(adGroupTargeting.FileInfo.Location.ToString(), Instance.Configuration.Options["Facebook.Ads.getAdGroupTargeting.xpath"]);
				using (adGroupTargetingReader)
				{
					while (adGroupTargetingReader.Read())
					{
						Ad ad = ads[adGroupTargetingReader.Current.adgroup_id];
						string age_min = adGroupTargetingReader.Current.age_min;
						if (!string.IsNullOrEmpty(age_min))
						{
							AgeTarget ageTarget = new AgeTarget() { FromAge = int.Parse(age_min), ToAge = int.Parse(adGroupTargetingReader.Current.age_max), OriginalID = adGroupTargetingReader.Current.adgroup_id };
							ad.Targets.Add(ageTarget);
						}
						XmlDynamicObject genders = adGroupTargetingReader.Current.genders as XmlDynamicObject;

						if (genders != null)
						{
							foreach (string gender in genders.GetArray("xsd:int"))
							{
								GenderTarget genderTarget = new GenderTarget();
								if (gender == "1")
									genderTarget.Gender = Gender.Male;
								else if (gender == "2")
									genderTarget.Gender = Gender.Female;
								else
									genderTarget.Gender = Gender.UnSpecified;

								genderTarget.OriginalID = gender;
								ad.Targets.Add(genderTarget);
							}

						}


					}
				

				}







				
				var creativeFiles = from dfc in this.Delivery.Files
									where dfc.Parameters.ContainsKey("IsCreativeDeliveryFile")
									select dfc;

				foreach (var creativeFile in creativeFiles)
				{
					var adGroupCreativesReader = new XmlDynamicReader
							(creativeFile.FileInfo.Location.ToString(), Instance.Configuration.Options["Facebook.Ads.AdGroupCreatives.xpath"]);// ./Ads_getAdGroupCreatives_response/ads_creative



					using (adGroupCreativesReader)
					{


						while (adGroupCreativesReader.Read())
						{

							Ad ad = ads[adGroupCreativesReader.Current.adgroup_id];
							ad.Creatives = new List<Creative>();//TODO: DISTENGUSHI BETWEEN CREATIVES UNIQUE ID DATABASE?
							ad.Creatives.Add(new ImageCreative()
							{
								ImageUrl = adGroupCreativesReader.Current.image_url,
								OriginalID = adGroupCreativesReader.Current.creative_id
							
							});
							ad.Creatives.Add(new TextCreative()
							{
								OriginalID = adGroupCreativesReader.Current.creative_id,
								TextType = TextCreativeType.Body,
								Text = adGroupCreativesReader.Current.body
														

							});
							ad.Creatives.Add(new TextCreative()
							{

								OriginalID = adGroupCreativesReader.Current.creative_id,
								TextType = TextCreativeType.DisplayUrl,
								Text = adGroupCreativesReader.Current.preview_url
								
							});
							ad.Creatives.Add(new TextCreative()
							{

								OriginalID = adGroupCreativesReader.Current.creative_id,
								TextType = TextCreativeType.Title,
								Text = adGroupCreativesReader.Current.title
								
							});



							session.ImportAd(ad);

							//TODO: REPORT PROGRESS 2	 ReportProgress(PROGRESS)
						}

						creativeFile.History.Add(DeliveryOperation.Processed, Instance.InstanceID);
					}

				}
				session.Commit();
			}
			return Core.Services.ServiceOutcome.Success;
		}

		private AdMetricsUnit CreateUnitFromFacebookData(AdData ad, CampaignData campaign, dynamic adGroupCreative)
		{
			AdMetricsUnit unit = new AdMetricsUnit();
			//unit.TimeStamp = this.Delivery.TargetPeriod; TODO: WHAT IS TIME STAMP
			unit.Account = new Account() { ID = Instance.AccountID };
			// Campaign
			unit.Campaign = new Campaign()
			{
				Name = campaign.CampaignName,
				OriginalID = campaign.CampaignID
			};

			// Ad
			unit.Ad = new Ad()
			{
				OriginalID = ad.adgroup_id,
				DestinationUrl = ad.link_url
				//TODO : TARGETS
			};
			// Targeting
			foreach (var target in ad.Targets)
			{
				unit.Ad.Targets.Add(target);
			}

			// Tracker
			unit.Tracker = new Tracker(unit.Ad);






			// Currency ??


			// MEASURES

			unit.Impressions = ad.Impressions;
			unit.Clicks = ad.Clicks;
			unit.Cost = ad.Cost;




			unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.Title, Value = adGroupCreative.title });
			unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.Body, Value = adGroupCreative.body });
			unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.DisplayUrl, Value = adGroupCreative.preview_url });
			unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.Image, Value = adGroupCreative.image_url });
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
		public List<Target> Targets = new List<Target>();


	}


}
