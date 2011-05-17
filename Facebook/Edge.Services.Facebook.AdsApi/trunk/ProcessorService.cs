using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Importing;

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
		public const int Actions_MeasureID = -666;
		public const int SocialSpent_MeasureID = -667;
		public const int SocialClicks_MeasureID = -668;
		public const int SocialImpression_MeasureID = -668;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			//Campaigns
			DeliveryFile campaigns = this.Delivery.Files["Campaigns"];

			var campaignsReader = new XmlDynamicReader
			(FileManager.Open(campaigns.GetFileInfo()), Instance.ParentInstance.Configuration.Options["Facebook.ads.getCampaigns.xpath"]);
			Dictionary<string, Campaign> campaignsData = new Dictionary<string, Campaign>();
			using (campaignsReader)
			{
				while (campaignsReader.Read())
				{
					Campaign camp = new Campaign()
					{

						Name = campaignsReader.Current.name,
						OriginalID = campaignsReader.Current.campaign_id,

						Channel = new Channel()
						{
							ID = 6
						},
						Account = new Account()
						{
							ID = Instance.AccountID
						}

					};
					int campaignStatus = int.Parse(campaignsReader.Current.campaign_status);
					switch (campaignStatus)
					{
						case 1:
							camp.Status = CampaignStatus.Active;
							break;
						case 2:
							camp.Status = CampaignStatus.Paused;
							break;
						case 3:
							camp.Status = CampaignStatus.Deleted;
							break;
					}
					campaignsData.Add(camp.OriginalID, camp);
				}
				campaigns.History.Add(DeliveryOperation.Processed, Instance.InstanceID);
			}
			this.ReportProgress(0.1);
			//GetAdGroups
			DeliveryFile adGroups = this.Delivery.Files["AdGroups"];

			var adGroupsReader = new XmlDynamicReader
			(FileManager.Open(adGroups.GetFileInfo()),
			Instance.ParentInstance.Configuration.Options["Facebook.ads.GetAdGroups.xpath"]);


			Dictionary<string, Ad> ads = new Dictionary<string, Ad>();
			using (adGroupsReader)
			{
				while (adGroupsReader.Read())
				{
					Ad ad = new Ad()
					{
						OriginalID = adGroupsReader.Current.ad_id,
						Campaign = campaignsData[adGroupsReader.Current.campaign_id],
						Name = adGroupsReader.Current.name



					};
					if (Instance.Configuration.Options.ContainsKey("AutoAdGroupSegment") && Instance.Configuration.Options["AutoAdGroupSegment"].ToLower() == "true")
					{
						string[] delimiter = new string[1];
						delimiter[0] = string.Empty;
						if (!Instance.Configuration.Options.ContainsKey("AdGroupDelimiter"))
							Edge.Core.Utilities.Log.Write(string.Format("Facebook{0}", this), Core.Utilities.LogMessageType.Warning);
						else
							delimiter[0] = Instance.Configuration.Options["AdGroupDelimiter"];

						ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
						{
							Value = delimiter[0]==string.Empty? ad.Name : ad.Name.Split(delimiter,StringSplitOptions.None)[0],
							OriginalID=ad.Name
						};
					}
					else
					{
						ad.Segments[Segment.AdGroupSegment] = new SegmentValue()
						{
							Value = ad.Name,
							OriginalID=ad.Name
						};
					}

					ads.Add(ad.OriginalID, ad);

				}
				adGroups.History.Add(DeliveryOperation.Processed, Instance.InstanceID);
			}


			//GetAdGroupStats
			DeliveryFile adGroupStats = this.Delivery.Files["AdGroupStats"];

			var adGroupStatsReader = new XmlDynamicReader
				(FileManager.Open(adGroupStats.GetFileInfo()), Instance.ParentInstance.Configuration.Options["Facebook.Ads.GetAdGroupStats.xpath"]);


			using (var session = new AdDataImportSession(this.Delivery))
			{
				session.Begin(true);
				using (adGroupStatsReader)
				{
					while (adGroupStatsReader.Read())
					{


						AdMetricsUnit adMetricsUnit = new AdMetricsUnit();
						//if (ads.ContainsKey(adGroupStatsReader.Current.id)) 
						adMetricsUnit.Ad = ads[adGroupStatsReader.Current.id];
						adMetricsUnit.Clicks = Convert.ToInt64(adGroupStatsReader.Current.clicks);
						adMetricsUnit.Cost = Convert.ToDouble(adGroupStatsReader.Current.spent);
						adMetricsUnit.Impressions = Convert.ToInt64(adGroupStatsReader.Current.impressions);
						adMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
						adMetricsUnit.Measures.Add(new Measure()
						{
							ID = SocialImpression_MeasureID,
							Account = new Account()
							{
								ID = int.Parse(this.Delivery.Parameters["AccountID"].ToString())
							},
							Name = "social_impressions"
						},
						double.Parse(adGroupStatsReader.Current.social_impressions));

						adMetricsUnit.Measures.Add(new Measure()
						{
							ID = SocialClicks_MeasureID,
							Account = new Account()
							{
								ID = int.Parse(this.Delivery.Parameters["AccountID"].ToString())
							},
							Name = "social_clicks"
						},
						double.Parse(adGroupStatsReader.Current.social_clicks));

						adMetricsUnit.Measures.Add(new Measure()
						{
							ID = SocialSpent_MeasureID,
							Account = new Account()
							{
								ID = int.Parse(this.Delivery.Parameters["AccountID"].ToString())
							},
							Name = "social_spent"
						},
						double.Parse(adGroupStatsReader.Current.social_spent));

						adMetricsUnit.Measures.Add(new Measure()
						{
							ID = Actions_MeasureID,
							Account = new Account()
							{
								ID = int.Parse(this.Delivery.Parameters["AccountID"].ToString())
							},
							Name = "actions"
						},
						double.Parse(adGroupStatsReader.Current.actions));
						adMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

						adMetricsUnit.Currency = new Currency()
						{
							Code = string.Empty
						};
						//adMetricsUnit.Conversions = new Dictionary<int, double>();
						adMetricsUnit.TargetMatches = new List<Target>();
						//TimeStamp=this.Delivery.TargetPeriod.Start.ExactDateTime




						session.ImportMetrics(adMetricsUnit);

					}

					adGroupStats.History.Add(DeliveryOperation.Processed, Instance.InstanceID); //TODO: HISTORY WHEN?PROCCESED IS AFTER DATABASE'?
					this.ReportProgress(0.4);
				}


				//getAdGroupTargeting

				DeliveryFile adGroupTargeting = this.Delivery.Files["AdGroupTargeting"];
				var adGroupTargetingReader = new XmlDynamicReader(FileManager.Open(adGroupTargeting.GetFileInfo()), Instance.ParentInstance.Configuration.Options["Facebook.Ads.getAdGroupTargeting.xpath"]);
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






				this.ReportProgress(0.6);

				var creativeFiles = from dfc in this.Delivery.Files
									where dfc.Parameters.ContainsKey("IsCreativeDeliveryFile")
									select dfc;

				foreach (var creativeFile in creativeFiles)
				{
					var adGroupCreativesReader = new XmlDynamicReader
							(FileManager.Open(creativeFile.GetFileInfo()), Instance.ParentInstance.Configuration.Options["Facebook.Ads.AdGroupCreatives.xpath"]);// ./Ads_getAdGroupCreatives_response/ads_creative



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
								//Name = adGroupCreativesReader.Current.name

							});
							ad.Creatives.Add(new TextCreative()
							{
								OriginalID = adGroupCreativesReader.Current.creative_id,
								TextType = TextCreativeType.Body,
								Text = adGroupCreativesReader.Current.body
								//Name = adGroupCreativesReader.Current.name


							});
							ad.Creatives.Add(new TextCreative()
							{

								OriginalID = adGroupCreativesReader.Current.creative_id,
								TextType = TextCreativeType.DisplayUrl,
								Text = adGroupCreativesReader.Current.preview_url
								//Name = adGroupCreativesReader.Current.name

							});
							ad.Creatives.Add(new TextCreative()
							{

								OriginalID = adGroupCreativesReader.Current.creative_id,
								TextType = TextCreativeType.Title,
								Text = adGroupCreativesReader.Current.title
								//Name = adGroupCreativesReader.Current.name

							});



							session.ImportAd(ad);

							//TODO: REPORT PROGRESS 2	 ReportProgress(PROGRESS)
						}

						creativeFile.History.Add(DeliveryOperation.Processed, Instance.InstanceID);
					}

				}
				session.Commit();
				this.ReportProgress(0.98);
			}
			return Core.Services.ServiceOutcome.Success;
		}

		//private AdMetricsUnit CreateUnitFromFacebookData(AdData ad, CampaignData campaign, dynamic adGroupCreative)
		//{
		//    AdMetricsUnit unit = new AdMetricsUnit();
		//    //unit.TimeStamp = this.Delivery.TargetPeriod; TODO: WHAT IS TIME STAMP
		//    unit.Account = new Account() { ID = Instance.AccountID };
		//    // Campaign
		//    unit.Campaign = new Campaign()
		//    {
		//        Name = campaign.CampaignName,
		//        OriginalID = campaign.CampaignID
		//    };

		//    // Ad
		//    unit.Ad = new Ad()
		//    {
		//        OriginalID = ad.adgroup_id,
		//        DestinationUrl = ad.link_url
		//        //TODO : TARGETS
		//    };
		//    // Targeting
		//    foreach (var target in ad.Targets)
		//    {
		//        unit.Ad.Targets.Add(target);
		//    }

		//    // Tracker
		//    unit.Tracker = new Tracker(unit.Ad);






		//    // Currency ??


		//    // MEASURES

		//    unit.Impressions = ad.Impressions;
		//    unit.Clicks = ad.Clicks;
		//    unit.Cost = ad.Cost;




		//    unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.Title, Value = adGroupCreative.title });
		//    unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.Body, Value = adGroupCreative.body });
		//    unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.DisplayUrl, Value = adGroupCreative.preview_url });
		//    unit.Ad.Creatives.Add(new Creative() { CreativeType = CreativeType.Image, Value = adGroupCreative.image_url });
		//    return unit;


		//}


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
