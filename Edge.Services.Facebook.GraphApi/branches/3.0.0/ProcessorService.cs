using System;
using System.Collections.Generic;
using System.Linq;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Core.Utilities;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Objects;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Misc;

namespace Edge.Services.Facebook.GraphApi
{
	public class ProcessorService : AutoMetricsProcessorService
	{
		private readonly Dictionary<string, Ad> _adCache = new Dictionary<string, Ad>();

		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			Log("Starting Google.AdWords.ProcessorService", LogMessageType.Debug);
			InitMappings();
			Mappings.OnMappingApplied = SetEdgeType;
			
			var filesByType = Delivery.Parameters["FilesByType"] as IDictionary<Consts.FileTypes, List<string>>;
			LoadAds(filesByType);
			Log("Ads loaded into local cache", LogMessageType.Debug);
			Progress = 0.2;

			using (ImportManager = new MetricsDeliveryManager(InstanceID, EdgeTypes, new MetricsDeliveryManagerOptions()))
			{
				ImportManager.BeginImport(Delivery, GetSampleMetrics());
				Log("Objects and Metrics tables are created", LogMessageType.Debug);
				Progress = 0.3;

				foreach (var filePath in filesByType[Consts.FileTypes.AdGroups])
				{
					using (var reader = new JsonDynamicReader(Delivery.Files[filePath].OpenContents(), "$.data[*].*"))
					{
						while (reader.Read())
						{
							ImportManager.ImportMetrics(GetMetrics(reader));
						}
					}
				}
				Progress = 0.8;
				Log("Start importing objects", LogMessageType.Debug);
				ImportManager.EndImport();
				Log("Finished importing objects", LogMessageType.Debug);
			}

			return ServiceOutcome.Success;
		}

		protected override MetricsUnit GetSampleMetrics()
		{
			using (var reader = new JsonDynamicReader(Configuration.SampleFilePath, "$.data[*].*"))
			{
				if (reader.Read())
				{
					return GetMetrics(reader);
				}
			}
			return null;
		}
		#endregion

		#region Private Methods

		private MetricsUnit GetMetrics(JsonDynamicReader reader)
		{
			var metricsUnit = new MetricsUnit
			{
				GetEdgeField = GetEdgeField,
				Output = Delivery.Outputs.First(),
				MeasureValues = new Dictionary<Measure, double>()
			};

			if (reader.Current.adgroup_id != null)
			{
				if (_adCache.ContainsKey(reader.Current.adgroup_id))
				{
					// set Ad
					metricsUnit.Ad = _adCache[reader.Current.adgroup_id];

					// set measures
					metricsUnit.MeasureValues.Add(GetMeasure("Clicks"), Convert.ToInt64(reader.Current.clicks));
					metricsUnit.MeasureValues.Add(GetMeasure("UniqueClicks"), Convert.ToInt64(reader.Current.unique_clicks));
					metricsUnit.MeasureValues.Add(GetMeasure("Impressions"), Convert.ToInt64(reader.Current.impressions));
					metricsUnit.MeasureValues.Add(GetMeasure("UniqueImpressions"), Convert.ToInt64(reader.Current.unique_impressions));
					metricsUnit.MeasureValues.Add(GetMeasure("Cost"), Convert.ToDouble(Convert.ToDouble(reader.Current.spent) / 100d));
					metricsUnit.MeasureValues.Add(GetMeasure("SocialImpressions"), double.Parse(reader.Current.social_impressions));
					metricsUnit.MeasureValues.Add(GetMeasure("SocialUniqueImpressions"), double.Parse(reader.Current.social_unique_impressions));
					metricsUnit.MeasureValues.Add(GetMeasure("SocialClicks"), double.Parse(reader.Current.social_clicks));
					metricsUnit.MeasureValues.Add(GetMeasure("SocialUniqueClicks"), double.Parse(reader.Current.social_unique_clicks));
					metricsUnit.MeasureValues.Add(GetMeasure("SocialCost"), Convert.ToDouble(reader.Current.social_spent) / 100d);
					metricsUnit.MeasureValues.Add(GetMeasure("Actions"), 0);

					
				}
				else
					Log(String.Format("Cannot find Ad '{0}'", reader.Current.adgroup_id), LogMessageType.Warning);
			}
			return metricsUnit;
		}

		private void LoadAds(IDictionary<Consts.FileTypes, List<string>> filesByType)
		{
			var campaignCache = LoadCampaigns(filesByType[Consts.FileTypes.Campaigns]);
			var creativeCache = LoadCreatives(filesByType[Consts.FileTypes.Creatives]);

			var delimiter = Configuration.Parameters.ContainsKey("AdGroupDelimiter") ? Configuration.Parameters.Get<string>("AdGroupDelimiter") : String.Empty;
			var autoSeg = Configuration.Parameters.ContainsKey("AutoAdGroupSegment") && Configuration.Parameters.Get<bool>("AutoAdGroupSegment") && delimiter != String.Empty;

			foreach (var filePath in filesByType[Consts.FileTypes.AdGroups])
			{
				using (var reader = new JsonDynamicReader(Delivery.Files[filePath].OpenContents(), "$.data[*].*"))
				{
					while (reader.Read())
					{
						// create ad
						var ad = new Ad
							{
								OriginalID = reader.Current.ad_id,
								Fields = new Dictionary<EdgeField, object>(),
								TargetDefinitions = new List<TargetDefinition>()
							};

						// set campaign and adGroup
						if (campaignCache.ContainsKey(reader.Current.campaign_id))
						{
							var campaign = campaignCache[reader.Current.campaign_id];
							var adGroup = new StringValue
								{
									Value = autoSeg ? reader.Current.name.Split(delimiter) : reader.Current.name,
									OriginalID = String.Format("{0}{1}{2}", reader.Current.name, campaign.OriginalID, Delivery.Account.ID),
									Fields = new Dictionary<EdgeField, object> { { GetExtraField("Campaign"), campaign } }
								};

							ad.Fields.Add(GetExtraField("Campaign"), campaign);
							ad.Fields.Add(GetExtraField("AdGroup"), adGroup);
						}
						else
							Log(String.Format("Cannot find Campaign '{0}' for Ad '{1}' in file '{2}'", reader.Current.campaign_id, ad.OriginalID, filePath), LogMessageType.Warning);

						// set creative definition
						if (reader.Current.creative_ids != null && reader.Current.creative_ids.Count > 0)
						{
							if (reader.Current.creative_ids.Count > 1)
								Log(String.Format("There are more than one Creative ({1}) for Ad '{0}'", ad.OriginalID, reader.Current.creative_ids.Count), LogMessageType.Error);

							var creativeDefId = reader.Current.creative_ids[0];
							if (!creativeCache.ContainsKey(creativeDefId))
								Log(String.Format("Cannot find Creative '{0}' for Ad '{2}' in file '{1}'", reader.Current.campaign_id, ad.OriginalID, filePath), LogMessageType.Warning);
							else
								ad.CreativeMatch = creativeCache[creativeDefId];
						}

						// targeting - age
						if (reader.Current.targeting.ContainsKey("age_min") && reader.Current.targeting.ContainsKey("age_max"))
						{
							ad.TargetDefinitions.Add(new TargetDefinition
								{
									Target = new AgeTarget { FromAge = int.Parse(reader.Current.targeting["age_min"]), ToAge = int.Parse(reader.Current.targeting["age_max"]) }
								});
						}
						// targeting - gender
						if (reader.Current.targeting.ContainsKey("genders"))
						{
							foreach (var gender in reader.Current.targeting["genders"])
							{
								ad.TargetDefinitions.Add(new TargetDefinition
								{
									Target = new GenderTarget { Gender = gender == "1" ? Gender.Male : gender == "2" ? Gender.Female : Gender.Unspecified }
								});
							}
						}
						_adCache.Add(ad.OriginalID, ad);
					}
				}
			}
		}

		private Dictionary<string, Campaign> LoadCampaigns(IEnumerable<string> filePaths)
		{
			var campaignList = new Dictionary<string, Campaign>();
			foreach (var campaignFilePath in filePaths)
			{
				using (var reader = new JsonDynamicReader(Delivery.Files[campaignFilePath].OpenContents(), "$.data[*].*"))
				{
					while (reader.Read())
					{
						var campaign = new Campaign
						{
							Name = reader.Current.name,
							OriginalID = Convert.ToString(reader.Current.campaign_id),
						};

						long campaignStatus = long.Parse(reader.Current.campaign_status);
						switch (campaignStatus)
						{
							case 1:
								campaign.Status = ObjectStatus.Active;
								break;
							case 2:
								campaign.Status = ObjectStatus.Paused;
								break;
							case 3:
								campaign.Status = ObjectStatus.Deleted;
								break;
						}
						campaignList.Add(campaign.OriginalID, campaign);
					}
				}

			}
			return campaignList;
		}

		private Dictionary<string, CreativeMatch> LoadCreatives(IEnumerable<string> filePaths)
		{
			var creativeDefList = new Dictionary<string, CreativeMatch>();

			foreach (var filePath in filePaths)
			{
				using (var reader = new JsonDynamicReader(Delivery.Files[filePath].OpenContents(), "$.data[*].*"))
				{
					while (reader.Read())
					{
						switch ((string)reader.Current.type)
						{
							case "1":
							case "2":
							case "3":
							case "4":
							case "12":
								{
									creativeDefList.Add(reader.Current.creative_id, GetCreativeMatch(reader));
									break;
								}
							case "8":
							case "9":
							case "10":
							case "16":
							case "17":
							case "19":
							case "25":
								{
									creativeDefList.Add(reader.Current.creative_id, GetCreativeMatch(reader, "Sponsored Story"));
									break;
								}
							case "27":
								{
									creativeDefList.Add(reader.Current.creative_id, GetCreativeMatch(reader, "Page Ads for a Page post"));
									break;
								}
							default:
								{
									creativeDefList.Add(reader.Current.creative_id, GetCreativeMatch(reader, "UnKnown creativet"));
									break;
								}
						}
					}
				}

			}
			return creativeDefList;
		}

		private CreativeMatch GetCreativeMatch(JsonDynamicReader reader, string text)
		{
			var creative = new TextCreative
			{
				//TextCreativeType = new TextCreativeType
				//{
				//	Value = "Text",
				//	TK = "Text",
				//	EdgeType = GetEdgeType("TextCreativeType")
				//},
				Text = text,
				Fields = new Dictionary<EdgeField, object> { { GetEdgeField("OriginalID"), reader.Current.creative_id } },
			};
			var creativeDef = new TextCreativeMatch
				{
					Creative = creative,
					Destination = new Destination { Value = text, TK = text }
				};
			return creativeDef;
		}

		private CreativeMatch GetCreativeMatch(JsonDynamicReader reader)
		{
			// composite creative
			var compCreative = new CompositeCreative
				{
					Parts = new Dictionary<CompositePartField, SingleCreative>()
				};
			var compCreativeDef = new CompositeCreativeMatch
				{
					Creative = compCreative,
					CreativesMatches = new Dictionary<CompositePartField, SingleCreativeMatch>()
				};

			// 1. image creative
			var creative = new ImageCreative
				{
					Image = reader.Current.image_url,
					Fields = new Dictionary<EdgeField, object> { { GetEdgeField("OriginalID"), reader.Current.creative_id } },
				};
			compCreative.Parts.Add(GetCompositePartField("ImageCreative"), creative);
			compCreativeDef.CreativesMatches.Add(GetCompositePartField("ImageCreativeDefinition"), new ImageCreativeMatch { Creative = creative });

			// 2. text creative Body
			var creativeDef = GetCreativeMatch(reader, reader.Current.Body);
			compCreative.Parts.Add(GetCompositePartField("Desc1Creative"), creativeDef.Creative);
			compCreativeDef.CreativesMatches.Add(GetCompositePartField("Desc1CreativeDefinition"), creativeDef);

			// 3. text creative Title
			creativeDef = GetCreativeMatch(reader, reader.Current.Title);
			compCreative.Parts.Add(GetCompositePartField("TitleCreative"), creativeDef.Creative);
			compCreativeDef.CreativesMatches.Add(GetCompositePartField("TitleCreativeDefinition"), creativeDef);

			return compCreativeDef;
		} 
		#endregion
	}
}
