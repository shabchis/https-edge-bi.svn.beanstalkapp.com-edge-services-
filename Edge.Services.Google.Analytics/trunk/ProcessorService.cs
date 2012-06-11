﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using Edge.Data.Pipeline.Mapping;
using Edge.Data.Pipeline.Metrics.GenericMetrics;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Common.Importing;

namespace Edge.Services.Google.Analytics
{
	class ProcessorService : MetricsProcessorServiceBase
	{
		DeliveryOutput currentOutput;
		bool _isChecksum = false;

		public new GenericMetricsImportManager ImportManager
		{
			get { return (GenericMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}

		protected override void OnInitMappings()
		{
			this.Mappings.ExternalMethods.Add("IsChecksum", new Func<bool>(() => _isChecksum));
		}
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			MappingContainer metricsUnitMapping = this.Mappings.Objects[typeof(GenericMetricsUnit)];
			currentOutput = this.Delivery.Outputs.First();
			currentOutput.Checksum = new Dictionary<string, double>();
			Dictionary<string, int> columns = new Dictionary<string, int>();
			foreach (var ReportFile in Delivery.Files)
			{

				//Get Columns
				var reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.columnHeaders[*].*");
				using (reportReader)
				{
					int colIndex = 0;
					while (reportReader.Read())
					{
						columns.Add(reportReader.Current.name, colIndex);
						colIndex++;
					}

					///sssss
				}

				using (this.ImportManager = new GenericMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
				{
					MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
					MeasureOptionsOperator = OptionsOperator.Not,
					SegmentOptions = Data.Objects.SegmentOptions.All,
					SegmentOptionsOperator = OptionsOperator.And
				}))
				{
					this.ImportManager.BeginImport(this.Delivery);
					
					Dictionary<string, GenericMetricsUnit> data = new Dictionary<string, GenericMetricsUnit>();
					
					// Checksums
					_isChecksum = true;
					reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.totalsForAllResults.*");
					using (reportReader)
					{
						this.Mappings.OnFieldRequired = field => reportReader.Current[field];
						if (reportReader.Read())
						{
							GenericMetricsUnit checksumUnit = new GenericMetricsUnit();
							metricsUnitMapping.Apply(checksumUnit);

							foreach (Measure measure in ImportManager.Measures.Values)
							{
								if (measure.Options.HasFlag(MeasureOptions.ValidationRequired))
									 currentOutput.Checksum.Add(measure.Name, checksumUnit.MeasureValues[measure]);
							}
						}
					}
					_isChecksum = false;

					//Get Valuees
					reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.rows[*].*");
					using (reportReader)
					{
						this.Mappings.OnFieldRequired = field => reportReader.Current["array"][field];

						while (reportReader.Read())
						{
							GenericMetricsUnit tempUnit = new GenericMetricsUnit();
							metricsUnitMapping.Apply(tempUnit);

							SegmentObject tracker = tempUnit.SegmentDimensions[ImportManager.SegmentTypes[Segment.Common.Tracker]];
							GenericMetricsUnit existingUnit = null;

							// check if we already found a metrics unit with the same tracker
							if (!data.TryGetValue(tracker.Value, out existingUnit))
							{
								tempUnit.Output = currentOutput;
								data.Add(tracker.Value, tempUnit);
							}
							else
							{
								// if tracker already exists, merge with existing values
								foreach (Measure measure in ImportManager.Measures.Values)
								{
									if (!measure.Options.HasFlag(MeasureOptions.IsBackOffice))
										continue;

									existingUnit.MeasureValues[measure] += tempUnit.MeasureValues[measure];
								}
							}
						}
					}

					// Import all unique units per tracker
					foreach (GenericMetricsUnit metricsUnit in data.Values)
					{
						this.ImportManager.ImportMetrics(metricsUnit);
					}

					ImportManager.EndImport();
				}
			}




			return Core.Services.ServiceOutcome.Success;
		}
	}
}
