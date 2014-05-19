using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Core.Utilities;
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Metrics;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Metrics.GenericMetrics;
using Edge.Data.Pipeline.Common.Importing;


namespace Edge.Services.Facebook.GraphApi
{

	public class ProcessorService : MetricsProcessorServiceBase
	{
		public new AdMetricsImportManager ImportManager
		{
			get { return (AdMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}
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
			Dictionary<string, Campaign> campaignsData = new Dictionary<string, Campaign>();
			Dictionary<string, Ad> ads = new Dictionary<string, Ad>();
			Dictionary<string, List<Ad>> adsBycreatives = new Dictionary<string, List<Ad>>();
			DeliveryOutput currentOutput = Delivery.Outputs.First();
			currentOutput.Checksum = new Dictionary<string, double>();
			using (this.ImportManager = new AdMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
			{

				MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
				MeasureOptionsOperator = OptionsOperator.Not,
				SegmentOptions = Data.Objects.SegmentOptions.All,
				SegmentOptionsOperator = OptionsOperator.And
			}))
			{
				this.ImportManager.BeginImport(this.Delivery);
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
								OriginalID = Convert.ToString(campaignsReader.Current.id),
							};

                            //string campaignStatus = campaignsReader.Current.campaign_status;
                            //switch (campaignStatus)
                            //{
                            //    case "ACTIVE":
                            //        camp.Status = ObjectStatus.Active;
                            //        break;
                            //    case "PAUSED":
                            //        camp.Status = ObjectStatus.Paused;
                            //        break;
                            //    case "DELETED":
                            //        camp.Status = ObjectStatus.Deleted;
                            //        break;
                            //}
							campaignsData.Add(camp.OriginalID, camp);
						}
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
							ad.OriginalID = Convert.ToString(adGroupsReader.Current.id);
							ad.Segments = new Dictionary<Segment, SegmentObject>();
							ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]] = campaignsData[Convert.ToString(adGroupsReader.Current.campaign_id)];

							ad.Name = adGroupsReader.Current.name;

							ad.Channel = new Channel()
							{
								ID = 6
							};

							ad.Account = new Account()
							{
								ID = this.Delivery.Account.ID,
								OriginalID = this.Delivery.Account.OriginalID.ToString()
							};


							if (Instance.Configuration.Options.ContainsKey("AutoAdGroupSegment") && Instance.Configuration.Options["AutoAdGroupSegment"].ToLower() == "true")
							{
								string[] delimiter = new string[1];
								delimiter[0] = string.Empty;
								if (!Instance.Configuration.Options.ContainsKey("AdGroupDelimiter"))
									Edge.Core.Utilities.Log.Write(string.Format("Facebook{0}", this), Core.Utilities.LogMessageType.Warning);
								else
									delimiter[0] = Instance.Configuration.Options["AdGroupDelimiter"];


                                //Note for Ronen - this section if relevant for AD SETS 
								ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.AdGroup]] = new AdGroup()
								{
									Campaign = (Campaign)ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]],
									Value = delimiter[0] == string.Empty ? ad.Name : ad.Name.Split(delimiter, StringSplitOptions.None)[0],
									OriginalID = delimiter[0] == string.Empty ? (ad.Name + ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]].OriginalID + ad.Account.ID) :
																(ad.Name.Split(delimiter, StringSplitOptions.None)[0] + ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]].OriginalID + ad.Account.ID)
								};
							}
							else
							{
								ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.AdGroup]] = new AdGroup()
								{
									Value = ad.Name,
									OriginalID = ad.Name + ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]].OriginalID + ad.Account.ID

								};

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

					}
				}
				#endregion


				#region AdGroupStats start new import session
				//GetAdGroupStats



				#region for validation

				foreach (var measure in this.ImportManager.Measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						if (!currentOutput.Checksum.ContainsKey(measure.Key))
							currentOutput.Checksum.Add(measure.Key, 0); //TODO : SHOULD BE NULL BUT SINCE CAN'T ADD NULLABLE ...TEMP

					}
				}

				#endregion


                if (filesByType.ContainsKey(Consts.FileTypes.ConversionsStats))
                {
                    List<string> convStatsFiles = filesByType[Consts.FileTypes.ConversionsStats];
                    foreach (var convStat in convStatsFiles)
                    {
                        DeliveryFile conversionStatsFile = this.Delivery.Files[convStat];

                        //Getting Next conversion file URL
                        var conversionStatsReader = new JsonDynamicReader(conversionStatsFile.OpenContents(), "$.data[*].*");

                        using (conversionStatsReader)
                        {
                            //Get Stats from conversion file
                        }
                    }

                }


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
								adMetricsUnit.Output = currentOutput;
								adMetricsUnit.MeasureValues = new Dictionary<Measure, double>();
								Ad tempAd;
								if (adGroupStatsReader.Current.adgroup_id != null)
								{
									if (ads.TryGetValue(adGroupStatsReader.Current.adgroup_id, out tempAd))
									{
										adMetricsUnit.Ad = tempAd;

										//adMetricsUnit.PeriodStart = this.Delivery.TimePeriodDefinition.Start.ToDateTime();
										//adMetricsUnit.PeriodEnd = this.Delivery.TimePeriodDefinition.End.ToDateTime();

										// Common and Facebook specific meausures

										/* Sets totals for validations */
										if (currentOutput.Checksum.ContainsKey(Measure.Common.Clicks))
											currentOutput.Checksum[Measure.Common.Clicks] += Convert.ToDouble(adGroupStatsReader.Current.clicks);
										if (currentOutput.Checksum.ContainsKey(Measure.Common.Impressions))
											currentOutput.Checksum[Measure.Common.Impressions] += Convert.ToDouble(adGroupStatsReader.Current.impressions);
										if (currentOutput.Checksum.ContainsKey(Measure.Common.Cost))
											currentOutput.Checksum[Measure.Common.Cost] += Convert.ToDouble(adGroupStatsReader.Current.spent) / 100d;

										/* Sets measures values */

										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Clicks], Convert.ToInt64(adGroupStatsReader.Current.clicks));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.UniqueClicks], Convert.ToInt64(adGroupStatsReader.Current.unique_clicks));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Impressions], Convert.ToInt64(adGroupStatsReader.Current.impressions));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.UniqueImpressions], Convert.ToInt64(adGroupStatsReader.Current.unique_impressions));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Cost], Convert.ToDouble(Convert.ToDouble(adGroupStatsReader.Current.spent) / 100d));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.SocialImpressions], double.Parse(adGroupStatsReader.Current.social_impressions));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.SocialUniqueImpressions], double.Parse(adGroupStatsReader.Current.social_unique_impressions));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.SocialClicks], double.Parse(adGroupStatsReader.Current.social_clicks));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.SocialUniqueClicks], double.Parse(adGroupStatsReader.Current.social_unique_clicks));
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.SocialCost], Convert.ToDouble(adGroupStatsReader.Current.social_spent) / 100d);
										adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.Actions], 0);
										//adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[MeasureNames.Connections], double.Parse(adGroupStatsReader.Current.connections));

										this.ImportManager.ImportMetrics(adMetricsUnit);
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
								this.Mappings.OnFieldRequired = field => adGroupCreativesReader.Current[field];
								while (adGroupCreativesReader.Read())
								{

									List<Ad> adsByCreativeID = null;
									if (adsBycreatives.ContainsKey(adGroupCreativesReader.Current.id))
									{
										if (!usedCreatives.ContainsKey(adGroupCreativesReader.Current.id))
										{
											usedCreatives.Add(adGroupCreativesReader.Current.id, adGroupCreativesReader.Current.id);
											adsByCreativeID = adsBycreatives[adGroupCreativesReader.Current.id];
										}
									}
									if (adsByCreativeID != null)
									{
										foreach (Ad ad in adsByCreativeID)
										{
                                            // Ronen , please see Naama's file of Ad types
                                            ad.DestinationUrl = adGroupCreativesReader.Current.object_url;
											
                                            /*Get Data from Mapping E.g Tracker*/
												if (this.Mappings != null && this.Mappings.Objects.ContainsKey(typeof(Ad)))
													this.Mappings.Objects[typeof(Ad)].Apply(ad);


											ad.Creatives = new List<Creative>();
											switch ((string)adGroupCreativesReader.Current.type)
											{
												//case "8": // deprecated
												//case "9":  // deprecated
												//case "10":  // deprecated
												//case "16":  // deprecated
												//case "17":  // deprecated
												//case "19":  // deprecated
												case "25":													
													{
														TextCreative sponserStory = new TextCreative()
														{
															OriginalID = adGroupCreativesReader.Current.id,
															TextType = TextCreativeType.Title,
															Text = "Sponsored Story",
														
															

														};
														ad.DestinationUrl = "Sponsored Story";
														ad.Creatives.Add(sponserStory);
														break;

													}
												case "27":
													{
														TextCreative sponserStory = new TextCreative()
														{
															OriginalID = adGroupCreativesReader.Current.id,
															TextType = TextCreativeType.Title,
															Text = "Page Ads for a Page post"

														};
														ad.DestinationUrl = "Page Ads for a Page post";
														ad.Creatives.Add(sponserStory);
														break;
													}
												
												case "1":
												case "2":
												case "3":
												case "4":
												case "12":
													{
														ImageCreative ic = new ImageCreative()
														{
															ImageUrl = adGroupCreativesReader.Current.image_url,
															OriginalID = adGroupCreativesReader.Current.id

															//Name = adGroupCreativesReader.Current.name

														};
														if (!string.IsNullOrEmpty(ic.ImageUrl))
															ad.Creatives.Add(ic);
														TextCreative bc = new TextCreative()
														{
															OriginalID = adGroupCreativesReader.Current.id,
															TextType = TextCreativeType.Body,
															Text = adGroupCreativesReader.Current.body
															//Name = adGroupCreativesReader.Current.name


														};
														if (!string.IsNullOrEmpty(bc.Text))
															ad.Creatives.Add(bc);

														//bug creative type =9 story like
														TextCreative tc = new TextCreative()
														{
															OriginalID = adGroupCreativesReader.Current.id,
															TextType = TextCreativeType.Title,
															Text = adGroupCreativesReader.Current.title
														};
														if (!string.IsNullOrEmpty(bc.Text))
															ad.Creatives.Add(tc);
														break;
													}
												default:
													{
														TextCreative unknown = new TextCreative()
														{
															OriginalID = adGroupCreativesReader.Current.id,
															TextType = TextCreativeType.Title,
															Text = "UnKnown creative"

														};
														ad.DestinationUrl = "UnKnown creative";
														ad.Creatives.Add(unknown);
														break;
													}

											}



											this.ImportManager.ImportAd(ad);
										}
									}

									//TODO: REPORT PROGRESS 2	 ReportProgress(PROGRESS)
								}


							}
						#endregion


						}
					}
				}
				currentOutput.Status = DeliveryOutputStatus.Imported;
				this.ImportManager.EndImport();
				if (!string.IsNullOrEmpty(warningsStr.ToString()))
					Log.Write(warningsStr.ToString(), LogMessageType.Warning);
			}
			return Core.Services.ServiceOutcome.Success;
		}




	}


}
