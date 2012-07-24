using System;
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
using Edge.Core.Utilities;

namespace Edge.Services.SalesForce
{
	class ProcessorService : MetricsProcessorServiceBase
	{
		DeliveryOutput currentOutput;


		public new GenericMetricsImportManager ImportManager
		{
			get { return (GenericMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}
		protected override void OnInitMappings()
		{
			this.Mappings.ExternalMethods.Add("GetConversion", new Func<object, int>(GetConversion));
		}
		protected int GetConversion(object d)
		{
			int result = 0;
			DateTime date;
			string dd = System.Convert.ToString(d);
			if (!string.IsNullOrEmpty(dd))
			{
				date = System.Convert.ToDateTime(dd);
				if (date.Date == Delivery.TimePeriodStart.Date)
					result = 1;

			}
			return result;



		}



		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			MappingContainer metricsUnitMapping;
			if (!this.Mappings.Objects.TryGetValue(typeof(GenericMetricsUnit), out metricsUnitMapping))
				throw new MappingConfigurationException("Missing mapping definition for GenericMetricsUnit.");
			currentOutput = this.Delivery.Outputs.First();
			currentOutput.Checksum = new Dictionary<string, double>();
			Dictionary<string, int> columns = new Dictionary<string, int>();
			foreach (var ReportFile in Delivery.Files)
			{

				using (this.ImportManager = new GenericMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
				{
					MeasureOptions = MeasureOptions.IsBackOffice,
					MeasureOptionsOperator = OptionsOperator.Or,
					SegmentOptions = Data.Objects.SegmentOptions.All,
					SegmentOptionsOperator = OptionsOperator.And
				}))
				{
					this.ImportManager.BeginImport(this.Delivery);

					Dictionary<string, GenericMetricsUnit> data = new Dictionary<string, GenericMetricsUnit>();
					//check number of recordes
					JsonDynamicReader reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.None), "$.totalSize");
					int numOfRecordes = 0;
					if (reportReader.Read())
						numOfRecordes = int.Parse(reportReader.Current.totalSize);


					if (numOfRecordes > 0)
					{
						//Get Valuees
						reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.None), "$.records[*].*");
						using (reportReader)
						{
							this.Mappings.OnFieldRequired = field => reportReader.Current[field];

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
									foreach (var m in tempUnit.MeasureValues)
									{
										if (!m.Key.Options.HasFlag(MeasureOptions.IsBackOffice))
											continue;

										existingUnit.MeasureValues[m.Key] += m.Value;
									}
								}
							}
						}


						// Import all unique units per tracker
						foreach (GenericMetricsUnit metricsUnit in data.Values)
						{
							this.ImportManager.ImportMetrics(metricsUnit);
						}
					}
					else
						Log.Write("No Data Found", LogMessageType.Information);

					ImportManager.EndImport();
				}
			}




			return Core.Services.ServiceOutcome.Success;
		}
	}
}
