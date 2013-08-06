using System;
using System.Collections.Generic;
using System.Linq;
using Edge.Core.Services;
using Edge.Core.Utilities;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Mapping;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Objects;

namespace Edge.Services.Google.AdWords.Settings
{
	/// <summary>
	/// Processor for CampaingCriterions (Language and Location)
	/// Only if single Language/Location is defined for Campaign insert it as ExtraField of Campaign, otherwise - nothing
	/// </summary>
	public class CampaignCriterionProcessorService : AutoMetricsProcessorService
	{
		#region Data Members
		private readonly Dictionary<string, List<RelationObject>> _campaignRelationMap = new Dictionary<string, List<RelationObject>>(); 
		#endregion

		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			// init mapping and load configuration
			Log("Starting Google.AdWords.CampaignCriterionProcessorService", LogMessageType.Debug);
			InitMappings();

			Mappings.OnMappingApplied = SetEdgeType;

			if (!Mappings.Objects.TryGetValue(typeof(MetricsUnit), out MetricsMappings))
				throw new MappingConfigurationException("Missing mapping definition for MetricsUnit.", "Object");

			LoadConfiguration();
			Progress = 0.1;

			// get nessesary EdgeFields
			var relationField = GetEdgeFieldByName("Relation");
			var locationField = GetEdgeFieldByName("Location");
			var languageField = GetEdgeFieldByName("Language");
			var isLocationNegativeField = GetEdgeFieldByName("IsLocationNegative");
			var isLanguageNegativeField = GetEdgeFieldByName("IsLanguageNegative");

			using (ReaderAdapter)
			{
				using (var stream = _deliveryFile.OpenContents(compression: _compression))
				{
					ReaderAdapter.Init(stream, Configuration);
					while (ReaderAdapter.Reader.Read())
					{
						// load metrics unit which contains Relation object of Campaign-Language or Campaign-Location
						CurrentMetricsUnit = new MetricsUnit {GetEdgeField = GetEdgeField, Output = new DeliveryOutput()};
						MetricsMappings.Apply(CurrentMetricsUnit);

						// insert loaded relation to List per Campaign
						var relation = CurrentMetricsUnit.Dimensions[relationField] as RelationObject;
						if (relation != null)
						{
							var campaign = relation.Object1 as Campaign;
							if (campaign != null)
							{
								if (!_campaignRelationMap.ContainsKey(campaign.OriginalID))
									_campaignRelationMap.Add(campaign.OriginalID, new List<RelationObject>());

								_campaignRelationMap[campaign.OriginalID].Add(relation);
							}
						}
					}
				}
			}
			Progress = 0.5;

			using (ImportManager = new MetricsDeliveryManager(InstanceID, EdgeTypes, _importManagerOptions) {OnLog = Log})
			{
				// create object tables
				ImportManager.BeginImport(Delivery, null);

				// add objects to EdgeObjectsManager cache
				PrepareImportObjects(x => x is Location, locationField, isLocationNegativeField);
				PrepareImportObjects(x => x is Language, languageField, isLanguageNegativeField);

				// import objects
				ImportManager.EndImport();
			}
			return ServiceOutcome.Success;
		}
		#endregion

		#region Private Methods
		private EdgeField GetEdgeFieldByName(string fieldName)
		{
			var field = EdgeFields.FirstOrDefault(x => x.Name == fieldName);
			if (field == null)
				throw new Exception(String.Format("Cannot find field '{0}' in EdgeFields table, please verify it is defined.", fieldName));
			return field;
		}

		/// <summary>
		/// Only if single Language/Location is defined per Campaign insert it as ExtraField of Campaign, otherwise - nothing
		/// Add Campaing and Language/Location of EdgeObjectsManager cache for futher Objects Import
		/// </summary>
		/// <param name="predicate">predicate to select only Language/Location objects</param>
		/// <param name="field"></param>
		/// <param name="isNegativeField"></param>
		private void PrepareImportObjects(Predicate<EdgeObject> predicate, EdgeField field, EdgeField isNegativeField)
		{
			foreach (var item in _campaignRelationMap.Where(x => x.Value.Count(y => predicate(y.Object2)) == 1))
			{
				// get campaign and single object (Language or Location) defined for Campaign
				var campaign = _campaignRelationMap[item.Key][0].Object1 as Campaign;
				var obj = _campaignRelationMap[item.Key].FirstOrDefault(x => predicate(x.Object2));

				if (campaign == null || obj == null || obj.Object2 == null) continue;

				if (campaign.Fields == null)
					campaign.Fields = new Dictionary<EdgeField, object>();

				campaign.Fields.Add(field, obj.Object2);
				campaign.Fields.Add(isNegativeField, _campaignRelationMap[item.Key][0].IsNegative);

				ImportManager.EdgeObjectsManager.AddToCache(NormalizeEdgeObject(campaign));
				ImportManager.EdgeObjectsManager.AddToCache(NormalizeEdgeObject(obj.Object2));
				if (obj.Object2 is Location)
					ImportManager.EdgeObjectsManager.AddToCache(NormalizeEdgeObject((obj.Object2 as Location).LocationType));
			}
		}

		/// <summary>
		/// Set EdgeObject Account adn Channel according to Metrics
		/// </summary>
		/// <param name="obj"></param>
		private EdgeObject NormalizeEdgeObject(EdgeObject obj)
		{
			if (CurrentMetricsUnit == null || obj == null) return null;

			obj.Account = CurrentMetricsUnit.Account;
			if (obj is ChannelSpecificObject)
				(obj as ChannelSpecificObject).Channel = CurrentMetricsUnit.Channel;

			return obj;
		} 
		#endregion
	}
}
