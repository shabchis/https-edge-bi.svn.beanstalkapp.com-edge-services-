using System;
using System.Collections.Generic;
using System.Configuration;
using Edge.Core.Services;
using Edge.Core.Utilities;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Mapping;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Misc;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Objects;
using Edge.Data.Objects;

namespace Edge.Services.SalesForce
{
	public class ProcessorService : AutoMetricsProcessorService
	{
		#region Override
		protected override ServiceOutcome DoPipelineWork()
		{
			Log("Starting SalesForce.ProcessorService", LogMessageType.Debug);
			InitMappings();
			Mappings.OnMappingApplied = SetEdgeType;

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

				var metricsMap = new Dictionary<string, MetricsUnit>();
				foreach (var file in Delivery.Files)
				{
					using (var reader = new JsonDynamicReader(file.OpenContents(), "$.records[*].*"))
					{
						Mappings.OnFieldRequired = fieldName => reader.Current[fieldName];
						Log(String.Format("Start reading metrics for file '{0}'", file.Name), LogMessageType.Debug);
						while (reader.Read())
						{
							// load current metrics unit
							LoadCurrentMetrics();

							if (CurrentMetricsUnit.Dimensions.ContainsKey(GetEdgeField("Tracker")) && CurrentMetricsUnit.Dimensions[GetEdgeField("Tracker")] is StringValue)
							{
								var tracker = CurrentMetricsUnit.Dimensions[GetEdgeField("Tracker")] as StringValue;
								// check if metrics unit already exists by tracker
								if (metricsMap.ContainsKey(tracker.Value))
								{
									// summaries all measure values
									foreach (var measure in CurrentMetricsUnit.MeasureValues)
									{
										metricsMap[tracker.Value].MeasureValues[measure.Key] += measure.Value;
									}
								}
								else
								{
									// add new metrics unit by tracker
									metricsMap.Add(tracker.Value, CurrentMetricsUnit);
								}
							}
							else
								Log(String.Format("Failed to get tracker in file '{0}'", file.Name), LogMessageType.Warning);
						}
					}
				}
				Progress = 0.5;
				
				// import summarized metrics 
				Log("Start importing summarizzed metrics", LogMessageType.Debug);
				foreach (var metrics in metricsMap)
				{
					ImportManager.ImportMetrics(metrics.Value);
				}
				Progress = 0.8;

				Log("Start importing objects", LogMessageType.Debug);
				ImportManager.EndImport();
			}
			return ServiceOutcome.Success;
		}

		protected override MetricsUnit GetSampleMetrics()
		{
			using (var reader = new JsonDynamicReader(Configuration.SampleFilePath, "$.records[*].*"))
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

		protected override void AddExternalMethods()
		{
			base.AddExternalMethods();
			Mappings.ExternalMethods.Add("GetConversion", new Func<object, int>(GetConversion));
		} 
		#endregion

		#region Scriptable Methods
		protected int GetConversion(dynamic dateStr)
		{
			var result = 0;
			if (dateStr != null && !string.IsNullOrEmpty(dateStr.ToString()))
			{
				var date = DateTime.Parse(dateStr.ToString());
				if (date.Date == Delivery.TimePeriodStart.Date)
					result = 1;
			}
			return result;
		} 
		#endregion
	}
}
