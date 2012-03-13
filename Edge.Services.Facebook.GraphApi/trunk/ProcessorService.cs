using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Core.Utilities;
using Edge.Services.AdMetrics;

namespace Edge.Services.Facebook.GraphApi
{

	public class ProcessorService : PipelineService
	{
		static class MeasureNames
		{
			public const string Actions = "Actions";
			public const string Connections = "Connections";
			public const string SocialCost = "SocialCost";
			public const string SocialClicks = "SocialClicks";
			public const string SocialUniqueClicks = "SocialUniqueClicks";
			public const string SocialImpressions = "SocialImpressions";
			public const string SocialUniqueImpressions = "SocialUniqueImpressions";
		}

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			Dictionary<Consts.FileTypes, List<string>> filesByType = (Dictionary<Consts.FileTypes, List<string>>)Delivery.Parameters["FilesByType"];
			StringBuilder warningsStr = new StringBuilder();
			Dictionary<string, double> _totalsValidation = new Dictionary<string, double>();
			Dictionary<string, Campaign> campaignsData = new Dictionary<string, Campaign>();
			Dictionary<string, Ad> ads = new Dictionary<string, Ad>();
			Dictionary<string, List<Ad>> adsBycreatives = new Dictionary<string, List<Ad>>();

			#region Campaigns
			List<string> campaignsFiles = filesByType[Consts.FileTypes.Campaigns];
			foreach (var campaignFile in campaignsFiles)
			{

				DeliveryFile campaigns = this.Delivery.Files[campaignFile];


				var campaignsReader = new JsonDynamicReader(campaigns.OpenContents(), "$.data[*].*");

				using (campaignsReader)
				{
					while (campaignsReader.Read())
					{

						Campaign camp = new Campaign()
						{

							Name = campaignsReader.Current.name,
							OriginalID = Convert.ToString(campaignsReader.Current.campaign_id),

							Channel = new Channel()
							{
								ID = 6
							},
							Account = new Account()
							{
								ID = this.Delivery.Account.ID,
								OriginalID = this.Delivery.Account.OriginalID.ToString()
							}

						};
						long campaignStatus = long.Parse(campaignsReader.Current.campaign_status);
						switch (campaignStatus)
						{
							case 1:
								camp.Status = ObjectStatus.Active;
								break;
							case 2:
								camp.Status = ObjectStatus.Paused;
								break;
							case 3:
								camp.Status = ObjectStatus.Deleted;
								break;
						}
						campaignsData.Add(camp.OriginalID, camp);
					}
					campaigns.History.Add(DeliveryOperation.Imported, Instance.InstanceID);
				}
			}
			#endregion
			this.ReportProgress(0.1);

			#region adGroups And Targeting
			List<string> adGroupsFiles = filesByType[Consts.FileTypes.AdGroups];
			foreach (var adGroup in adGroupsFiles)
			{
				DeliveryFile adGroups = this.Delivery.Files[adGroup];

				var adGroupsReader = new JsonDynamicReader(FileManager.Open(adGroups.Location), "$.data[*].*");




				using (adGroupsReader)
				{
					while (adGroupsReader.Read())
					{
						Ad ad = new Ad();
						ad.OriginalID = Convert.ToString(adGroupsReader.Current.ad_id);
						ad.Campaign = campaignsData[Convert.ToString(adGroupsReader.Current.campaign_id)];
						ad.Name = adGroupsReader.Current.name;


						if (Instance.Configuration.Options.ContainsKey("AutoAdGroupSegment") && Instance.Configuration.Options["AutoAdGroupSegment"].ToLower() == "true")
						{
							string[] delimiter = new string[1];
							delimiter[0] = string.Empty;
							if (!Instance.Configuration.Options.ContainsKey("AdGroupDelimiter"))
								Edge.Core.Utilities.Log.Write(string.Format("Facebook{0}", this), Core.Utilities.LogMessageType.Warning);
							else
								delimiter[0] = Instance.Configuration.Options["AdGroupDelimiter"];

							ad.Segments[Segment.AdGroupSegment] = new SegmentValue();

							ad.Segments[Segment.AdGroupSegment].Value = delimiter[0] == string.Empty ? ad.Name : ad.Name.Split(delimiter, StringSplitOptions.None)[0];
							ad.Segments[Segment.AdGroupSegment].OriginalID = delimiter[0] == string.Empty ? (ad.Name + ad.Campaign.OriginalID + ad.Campaign.Account.ID) :
							(ad.Name.Split(delimiter, StringSplitOptions.None)[0] + ad.Campaign.OriginalID + ad.Campaign.Account.ID);

						}
						else
						{
							ad.Segments[Segment.AdGroupSegment] = new SegmentValue();
							ad.Segments[Segment.AdGroupSegment].Value = ad.Name;
							ad.Segments[Segment.AdGroupSegment].OriginalID = ad.Name + ad.Campaign.OriginalID + ad.Campaign.Account.ID;
						}
						// adgroup targeting
						string age_min = string.Empty;
						if (((Dictionary<string, object>)adGroupsReader.Current.targeting).ContainsKey("age_min"))
							age_min = adGroupsReader.Current.targeting["age_min"];

						if (!string.IsNullOrEmpty(age_min))
						{
							AgeTarget ageTarget = new AgeTarget() { FromAge = int.Parse(age_min), ToAge = int.Parse(adGroupsReader.Current.targeting["age_max"]) };
							ad.Targets.Add(ageTarget);
						}
						List<object> genders = null;
						if (((Dictionary<string, object>)adGroupsReader.Current.targeting).ContainsKey("genders"))
							genders = adGroupsReader.Current.targeting["genders"];

						if (genders != null)
						{
							foreach (object gender in genders)
							{
								GenderTarget genderTarget = new GenderTarget();
								if (gender.ToString() == "1")
									genderTarget.Gender = Gender.Male;
								else if (gender.ToString() == "2")
									genderTarget.Gender = Gender.Female;
								else
									genderTarget.Gender = Gender.Unspecified;

								genderTarget.OriginalID = gender.ToString();
								ad.Targets.Add(genderTarget);

							}

						}
						if (adGroupsReader.Current.creative_ids != null)
						{
							foreach (string creative in adGroupsReader.Current.creative_ids)
							{
								if (!adsBycreatives.ContainsKey(creative))
									adsBycreatives.Add(creative, new List<Ad>());
								adsBycreatives[creative].Add(ad);

							}
						}
						ads.Add(ad.OriginalID, ad);
					}
					adGroups.History.Add(DeliveryOperation.Imported, Instance.InstanceID);
				}
			}
			#endregion


			#region AdGroupStats start new import session
			//GetAdGroupStats
			using (var session = new AdMetricsImportManager(this.Instance.InstanceID))
			{

				session.BeginImport(this.Delivery);
				#region for validation

				foreach (var measure in session.Measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						if (!_totalsValidation.ContainsKey(measure.Key))
							_totalsValidation.Add(measure.Key, 0); //TODO : SHOULD BE NULL BUT SINCE CAN'T ADD NULLABLE ...TEMP

					}


				}



				#endregion

				if (filesByType.ContainsKey(Consts.FileTypes.AdGroupStats))
				{
					List<string> adGroupStatsFiles = filesByType[Consts.FileTypes.AdGroupStats];
					foreach (var adGroupStat in adGroupStatsFiles)
					{

						DeliveryFile adGroupStats = this.Delivery.Files[adGroupStat];

						var adGroupStatsReader = new JsonDynamicReader(adGroupStats.OpenContents(), "$.data[*].*");




						using (adGroupStatsReader)
						{
							while (adGroupStatsReader.Read())
							{
								AdMetricsUnit adMetricsUnit = new AdMetricsUnit();
								Ad tempAd;
								if (adGroupStatsReader.Current.adgroup_id != null)
								{
									if (ads.TryGetValue(adGroupStatsReader.Current.adgroup_id, out tempAd))
									{
										adMetricsUnit.Ad = tempAd;

										adMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
										adMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

										// Common and Facebook specific meausures
										if (_totalsValidation.ContainsKey(Measure.Common.Clicks))
											_totalsValidation[Measure.Common.Clicks] += Convert.ToDouble(adGroupStatsReader.Current.clicks);
										adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Clicks]] = Convert.ToInt64(adGroupStatsReader.Current.clicks);
										adMetricsUnit.MeasureValues[session.Measures[Measure.Common.UniqueClicks]] = Convert.ToInt64(adGroupStatsReader.Current.unique_clicks);
										if (_totalsValidation.ContainsKey(Measure.Common.Impressions))
											_totalsValidation[Measure.Common.Impressions] += Convert.ToDouble(adGroupStatsReader.Current.impressions);
										adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Impressions]] = Convert.ToInt64(adGroupStatsReader.Current.impressions);
										adMetricsUnit.MeasureValues[session.Measures[Measure.Common.UniqueImpressions]] = Convert.ToInt64(adGroupStatsReader.Current.unique_impressions);
										if (_totalsValidation.ContainsKey(Measure.Common.Cost))
											_totalsValidation[Measure.Common.Cost] += Convert.ToDouble(adGroupStatsReader.Current.spent) / 100d;
										adMetricsUnit.MeasureValues[session.Measures[Measure.Common.Cost]] = Convert.ToDouble(Convert.ToDouble(adGroupStatsReader.Current.spent) / 100d);
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.SocialImpressions], double.Parse(adGroupStatsReader.Current.social_impressions));
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.SocialUniqueImpressions], double.Parse(adGroupStatsReader.Current.social_unique_impressions));
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.SocialClicks], double.Parse(adGroupStatsReader.Current.social_clicks));
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.SocialUniqueClicks], double.Parse(adGroupStatsReader.Current.social_unique_clicks));
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.SocialCost], Convert.ToDouble(adGroupStatsReader.Current.social_spent) / 100d);
										
										
										
										//adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.Actions], double.Parse(adGroupStatsReader.Current.actions));
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.Actions], 0);
										adMetricsUnit.MeasureValues.Add(session.Measures[MeasureNames.Connections], double.Parse(adGroupStatsReader.Current.connections));


										adMetricsUnit.TargetMatches = new List<Target>();

										session.ImportMetrics(adMetricsUnit);
									}
									else
									{
										warningsStr.AppendLine(string.Format("Ad {0} does not exist in the stats report delivery id: {1}", adGroupStatsReader.Current.id, this.Delivery.DeliveryID));


									}
								}
								else
								{
									warningsStr.AppendLine("adGroupStatsReader.Current.id=null");
								}

							}

							adGroupStats.History.Add(DeliveryOperation.Imported, Instance.InstanceID); //TODO: HISTORY WHEN?PROCCESED IS AFTER DATABASE'?
							this.ReportProgress(0.4);
						}

			#endregion








						this.ReportProgress(0.6);

						#region Creatives
						List<string> creativeFiles = filesByType[Consts.FileTypes.Creatives];

						Dictionary<string, string> usedCreatives = new Dictionary<string, string>();
						foreach (string creative in creativeFiles)
						{
							DeliveryFile creativeFile = Delivery.Files[creative];
							var adGroupCreativesReader = new JsonDynamicReader(creativeFile.OpenContents(), "$.data[*].*");



							using (adGroupCreativesReader)
							{
								while (adGroupCreativesReader.Read())
								{

									List<Ad> adsByCreativeID = null;
									if (adsBycreatives.ContainsKey(adGroupCreativesReader.Current.creative_id))
									{
										if (!usedCreatives.ContainsKey(adGroupCreativesReader.Current.creative_id))
										{
											usedCreatives.Add(adGroupCreativesReader.Current.creative_id, adGroupCreativesReader.Current.creative_id);
											adsByCreativeID = adsBycreatives[adGroupCreativesReader.Current.creative_id];
										}
									}
									if (adsByCreativeID != null)
									{
										foreach (Ad ad in adsByCreativeID)
										{

											ad.DestinationUrl = adGroupCreativesReader.Current.link_url;

											if (!string.IsNullOrEmpty(ad.DestinationUrl))
											{
												SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, ad.DestinationUrl);
												if (tracker != null)
													ad.Segments[Segment.TrackerSegment] = tracker;
											}

											ad.Creatives = new List<Creative>();
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
										}
									}

									//TODO: REPORT PROGRESS 2	 ReportProgress(PROGRESS)
								}

								creativeFile.History.Add(DeliveryOperation.Imported, Instance.InstanceID);
							}
						#endregion


						}
					}
				}
				session.HistoryEntryParameters.Add(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, _totalsValidation);
				session.EndImport();
				if (!string.IsNullOrEmpty(warningsStr.ToString()))
					Log.Write(warningsStr.ToString(), LogMessageType.Warning);
			}
			return Core.Services.ServiceOutcome.Success;
		}




	}


}
