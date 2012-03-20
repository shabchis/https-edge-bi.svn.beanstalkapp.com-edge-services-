﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Services.SegmentMetrics;
using Edge.Data.Objects;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

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
					Dictionary<string, SegmentMetricsUnit> data = new Dictionary<string, SegmentMetricsUnit>();
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
							SegmentMetricsUnit metricsUnit;
							SegmentValue tracker=null;
							int i = 1;
							string trackerField = null;
							while (Instance.Configuration.Options.ContainsKey(string.Format("SegmentField{0}", i)))
							{

								trackerField = Instance.Configuration.Options[string.Format("SegmentField{0}", i)];
								string value = reportReader.Current["array"][columns[trackerField]];

								tracker = AutoSegments.ExtractSegmentValue(Segment.TrackerSegment, value, trackerField);								
								if (tracker != null)
									break;
								i++;


							}
							if (tracker == null)
							{
								tracker = new SegmentValue() { Value = "0" };
							}
							if (!data.ContainsKey(tracker.Value))
								data.Add(tracker.Value, new SegmentMetricsUnit());
							metricsUnit = data[tracker.Value];
							metricsUnit.Segments[Segment.TrackerSegment] = tracker;

							foreach (var measure in session.Measures.Values)
							{
								if (string.IsNullOrEmpty(measure.SourceName) && measure.Account != null)
									throw new Exception(string.Format("Undifined Source Name in DB for measure {0} ", measure.Name));

								if (measure.Account != null)
								{
									if (!metricsUnit.MeasureValues.ContainsKey(session.Measures[measure.Name]))
										metricsUnit.MeasureValues.Add(session.Measures[measure.Name], 0);
									metricsUnit.MeasureValues[session.Measures[measure.Name]] += Convert.ToDouble(reportReader.Current["array"][columns[measure.SourceName]]);
								}

							}

							metricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
							metricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();


						}
					}
					foreach (SegmentMetricsUnit metricsUnit in data.Values)
					{
						Dictionary<string, string> usid = new Dictionary<string, string>();
						foreach (var segment in metricsUnit.Segments)
						{
							usid.Add(segment.Key.Name, segment.Value.Value);
						}
						session.ImportMetrics(metricsUnit, JsonConvert.SerializeObject(usid));
					}

					session.HistoryEntryParameters.Add(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, _totalsValidation);
					session.EndImport();
				}
			}




			return Core.Services.ServiceOutcome.Success;
		}
	}
}
