using System;
using System.Collections.Generic;
using System.Configuration;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Core.Utilities;
using Edge.Data.Pipeline.Mapping;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Objects;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Misc;

namespace Edge.Services.Facebook.GraphApi
{
	public class ProcessorService : AutoMetricsProcessorService
	{
		#region Data Members
		private readonly Dictionary<string, Ad> _adCache = new Dictionary<string, Ad>();
		private readonly Dictionary<string, Campaign> _campaignCache = new Dictionary<string, Campaign>();
		private readonly Dictionary<string, CompositeCreative> _creativeCache = new Dictionary<string, CompositeCreative>();

		protected MappingContainer AdMappings;
		protected MappingContainer CampaignMappings;
		protected MappingContainer CreativeMappings; 
		#endregion
		
		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			Log("Starting Facebook.GraphApi.ProcessorService", LogMessageType.Debug);
			InitMappings();
			Mappings.OnMappingApplied = SetEdgeType;

			if (!Mappings.Objects.TryGetValue(typeof(Ad), out AdMappings))
				throw new MappingConfigurationException("Missing mapping definition for Ad.", "Object");
			
			if (!Mappings.Objects.TryGetValue(typeof(Campaign), out CampaignMappings))
				throw new MappingConfigurationException("Missing mapping definition for Campaign.", "Object");

			if (!Mappings.Objects.TryGetValue(typeof(CompositeCreative), out CreativeMappings))
				throw new MappingConfigurationException("Missing mapping definition for CompositeCreative.", "Object");

			if (!Mappings.Objects.TryGetValue(typeof(MetricsUnit), out MetricsMappings))
				throw new MappingConfigurationException("Missing mapping definition for MetricsUnit.", "Object");

			if (!Mappings.Objects.TryGetValue(typeof(Signature), out SignatureMappings))
				throw new MappingConfigurationException("Missing mapping definition for Signature.", "Object");
			
			using (ImportManager = new MetricsDeliveryManager(InstanceID, EdgeTypes, new MetricsDeliveryManagerOptions()))
			{
				// create objects and metrics table according to the sample metrics
				ImportManager.BeginImport(Delivery, GetSampleMetrics());
				Log("Objects and Metrics tables are created", LogMessageType.Debug);
				Progress = 0.1;

				// load Campaigns, Creatives and Ads into local Cache
				ClearLocalCache();
				var filesByType = Delivery.Parameters["FilesByType"] as IDictionary<Consts.FileTypes, List<string>>;
				LoadFiles(filesByType[Consts.FileTypes.Campaigns], LoadCampaigns);
				LoadFiles(filesByType[Consts.FileTypes.Creatives], LoadCreatives);
				LoadFiles(filesByType[Consts.FileTypes.AdGroups], LoadAds);

				Log("Campaigns, Creatives and Ads are loaded into local cache", LogMessageType.Debug);
				Progress = 0.3;

				// start processing metrics
				foreach (var filePath in filesByType[Consts.FileTypes.AdGroupStats])
				{
					using (var reader = new JsonDynamicReader(Delivery.Files[filePath].OpenContents(), "$.data[*].*"))
					{
						Mappings.OnFieldRequired = fieldName => reader.Current[fieldName];
						while (reader.Read())
						{
							ProcessMetrics();
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

		protected override void AddExternalMethods()
		{
			base.AddExternalMethods();
			Mappings.ExternalMethods.Add("GetAd", new Func<dynamic, Ad>(GetAd));
			Mappings.ExternalMethods.Add("GetCampaign", new Func<dynamic, Campaign>(GetCampaign));
			Mappings.ExternalMethods.Add("GetCreative", new Func<dynamic, CompositeCreative>(GetCreative));
		}

		protected override MetricsUnit GetSampleMetrics()
		{
			LoadCampaigns(new DeliveryFile {Location = Configuration.Parameters.Get<string>("CampaignSampleFile")});
			LoadCreatives(new DeliveryFile {Location = Configuration.Parameters.Get<string>("CreativeSampleFile")});
			LoadAds      (new DeliveryFile {Location = Configuration.Parameters.Get<string>("AdGroupSampleFile")});

			using (var reader = new JsonDynamicReader(Configuration.SampleFilePath, "$.data[*].*"))
			{
				Mappings.OnFieldRequired = fieldName => reader.Current[fieldName];
				if (reader.Read())
				{
					CurrentMetricsUnit = new MetricsUnit { GetEdgeField = GetEdgeField, Output = new DeliveryOutput() };
					MetricsMappings.Apply(CurrentMetricsUnit);
					return CurrentMetricsUnit;
				}
			}
			throw new ConfigurationErrorsException(String.Format("Failed to read sample metrics from file: {0}", Configuration.SampleFilePath));
		}
		#endregion

		#region Private Methods

		private void LoadFiles(IEnumerable<string> filePaths, Action<DeliveryFile> loadingMethod)
		{
			foreach (var filePath in filePaths)
			{
				loadingMethod(Delivery.Files[filePath]);
			}
		}

		private void LoadAds(DeliveryFile file)
		{
			using (var reader = new JsonDynamicReader(file.OpenContents(), "$.data[*].*"))
			{
				Mappings.OnFieldRequired = fieldName =>
				{
					var parts = fieldName.Split('@');
					if (parts.Length > 1)
					{
						// value from the dictionary by key
						var fieldValue = reader.Current[parts[1]].ContainsKey(parts[0]) ?
											 reader.Current[parts[1]][parts[0]] is List<object> ?
											 String.Join(",", reader.Current[parts[1]][parts[0]]) :
										 reader.Current[parts[1]][parts[0]] : null;
						return fieldValue;
					}

					// 1st item in the list
					if (reader.Current[fieldName] is IList<object>)
						return (reader.Current[fieldName] as IList<object>)[0];

					// just a string value
					return reader.Current[fieldName];
				};

				while (reader.Read())
				{
					var ad = new Ad();
					AdMappings.Apply(ad);
					_adCache.Add(ad.OriginalID, ad);
				}
			}
		}

		private void LoadCampaigns(DeliveryFile file)
		{
			using (var reader = new JsonDynamicReader(file.OpenContents(), "$.data[*].*"))
			{
				Mappings.OnFieldRequired = fieldName => reader.Current[fieldName];
				while (reader.Read())
				{
					var campaign = new Campaign();
					CampaignMappings.Apply(campaign);

					_campaignCache.Add(campaign.OriginalID, campaign);
				}
			}
		}

		private void LoadCreatives(DeliveryFile file)
		{
			using (var reader = new JsonDynamicReader(file.OpenContents(), "$.data[*].*"))
			{
				Mappings.OnFieldRequired = fieldName =>
				{
					try
					{
						return reader.Current[fieldName];
					}
					catch (Exception ex)
					{
						Log(String.Format("Failed to read field '{0}' for creative id = {1} in file {2}, ex: {3}", fieldName, reader.Current.creative_id, file.Name, ex.Message), LogMessageType.Error);
						return null;
					}
				};
				while (reader.Read())
				{
					var creative = new CompositeCreative();
					CreativeMappings.Apply(creative);

					_creativeCache.Add(reader.Current.creative_id, creative);
				}
			}
		}

		private void ClearLocalCache()
		{
			_adCache.Clear();
			_creativeCache.Clear();
			_campaignCache.Clear();
		}

		#endregion

		#region Scriptable Methods
		private Ad GetAd(dynamic adId)
		{
			if (_adCache.ContainsKey(adId))
				return _adCache[adId];
			
			Log(String.Format("Ad '{0}' was not found in local cache", adId), LogMessageType.Warning);
			return null;
		}

		private Campaign GetCampaign(dynamic campaignId)
		{
			if (_campaignCache.ContainsKey(campaignId))
				return _campaignCache[campaignId];
			
			Log(String.Format("Campaign '{0}' was not found in local cache", campaignId), LogMessageType.Warning);
			return null;
		}

		private CompositeCreative GetCreative(dynamic creativeId)
		{
			if (_creativeCache.ContainsKey(creativeId))
				return _creativeCache[creativeId];

			Log(String.Format("Creative '{0}' was not found in local cache", creativeId), LogMessageType.Warning);
			return null;
		}
		#endregion
	}
}
