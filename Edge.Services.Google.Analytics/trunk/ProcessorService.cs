using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using Edge.Data.Pipeline.Metrics.GenericMetrics;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Common.Importing;

namespace Edge.Services.Google.Analytics
{
	class ProcessorService : MetricsProcessorServiceBase
	{
		DeliveryOutput currentOutput;
		public new GenericMetricsImportManager ImportManager
		{
			get { return (GenericMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			
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
					//get totals
					reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.totalsForAllResults.*");
					using (reportReader)
					{

						while (reportReader.Read())
						{

							foreach (var measure in ImportManager.Measures)
							{

								if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
									 currentOutput.Checksum.Add(measure.Value.SourceName, Convert.ToDouble(reportReader.Current[measure.Value.SourceName]));
							}
						}
					}

					//Get Valuees
					reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.rows[*].*");
					using (reportReader)
					{

						while (reportReader.Read())
						{
							GenericMetricsUnit metricsUnit;
							SegmentObject tracker=null;
							Segment segment;
							ImportManager.SegmentTypes.TryGetValue(Segment.Common.Tracker,out segment);
							int i = 1;
							string trackerField = null;
							while (Instance.Configuration.Options.ContainsKey(string.Format("SegmentField{0}", i)))
							{

								trackerField = Instance.Configuration.Options[string.Format("SegmentField{0}", i)];
								string value = reportReader.Current["array"][columns[trackerField]];

								tracker = this.Mappings.Objects[typeof(Ad)].Apply(ad);
								tracker = AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, value, trackerField);								
								if (tracker != null)
									break;
								i++;


							}
							if (tracker == null)
							{
								tracker = new SegmentObject() { Value = "0" };
							}
							if (!data.ContainsKey(tracker.Value))
								data.Add(tracker.Value, new GenericMetricsUnit());
							metricsUnit = data[tracker.Value];
							metricsUnit.SegmentDimensions[segment] = tracker;

							foreach (var measure in ImportManager.Measures.Values)
							{
								if (string.IsNullOrEmpty(measure.SourceName) && measure.Account != null)
									throw new Exception(string.Format("Undifined Source Name in DB for measure {0} ", measure.Name));

								if (measure.Account != null)
								{
									if (!metricsUnit.MeasureValues.ContainsKey(ImportManager.Measures[measure.Name]))
										metricsUnit.MeasureValues.Add(ImportManager.Measures[measure.Name], 0);
									metricsUnit.MeasureValues[ImportManager.Measures[measure.Name]] += Convert.ToDouble(reportReader.Current["array"][columns[measure.SourceName]]);
								}

							}
							metricsUnit.Output = currentOutput;
							metricsUnit.TimePeriodStart = this.Delivery.TimePeriodDefinition.Start.ToDateTime();
							metricsUnit.TimePeriodEnd = this.Delivery.TimePeriodDefinition.End.ToDateTime();


						}
					}
					foreach (GenericMetricsUnit metricsUnit in data.Values)
					{
						Dictionary<string, string> usid = new Dictionary<string, string>();
						foreach (var segment in metricsUnit.SegmentDimensions)
						{
							usid.Add(segment.Key.Name, segment.Value.Value);
						}

						//this.ImportManager.ImportMetrics(metricsUnit, JsonConvert.SerializeObject(usid));
						this.ImportManager.ImportMetrics(metricsUnit);
					}


					ImportManager.EndImport();
				}
			}




			return Core.Services.ServiceOutcome.Success;
		}
	}
}
