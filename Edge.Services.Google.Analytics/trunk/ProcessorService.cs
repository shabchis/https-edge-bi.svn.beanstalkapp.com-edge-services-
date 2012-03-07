using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Services.SegmentMetrics;
using Edge.Data.Objects;
using Newtonsoft.Json;

namespace Edge.Services.Google.Analytics
{
	class ProcessorService : PipelineService
	{
		Dictionary<string, double> _totalsValidation = new Dictionary<string, double>();
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

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

				using (var session = new SegmentMetricsImportManager(this.Instance.InstanceID))
				{
					session.BeginImport(this.Delivery);
					//get totals
					reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.totalsForAllResults.*");
					using (reportReader)
					{

						while (reportReader.Read())
						{

							foreach (var measure in session.Measures)
							{

								if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
									_totalsValidation.Add(measure.Value.SourceName, Convert.ToDouble(reportReader.Current[measure.Value.SourceName]));
							}
						}
					}

					//Get Valuees
					reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.Gzip), "$.rows[*].*");
					using (reportReader)
					{

						while (reportReader.Read())
						{
							SegmentMetricsUnit MetricsUnit = new SegmentMetricsUnit();
							StringBuilder trackerBuilder = new StringBuilder();
							SegmentValue tracker = this.AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, reportReader.Current["array"][columns["ga:adDestinationUrl"]]);
							if (tracker != null)
								MetricsUnit.Segments[Segment.TrackerSegment] = tracker;

							foreach (var measure in session.Measures.Values)
							{
								if (string.IsNullOrEmpty(measure.SourceName) && measure.Account != null)
									throw new Exception(string.Format("Undifined Source Name in DB for measure {0} ", measure.Name));

								if (measure.Account != null)
									MetricsUnit.MeasureValues[session.Measures[measure.Name]] = Convert.ToDouble(reportReader.Current["array"][columns[measure.SourceName]]);

							}
							Dictionary<string, string> usid = new Dictionary<string, string>();
							foreach (var segment in MetricsUnit.Segments)
							{
								usid.Add(segment.Key.Name, segment.Value.Value);
							}
							MetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
							MetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();
							session.ImportMetrics(MetricsUnit, JsonConvert.SerializeObject(usid));
						}
					}
					session.HistoryEntryParameters.Add(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, _totalsValidation);
					session.EndImport();
				}
			}




			return Core.Services.ServiceOutcome.Success;
		}
	}
}
