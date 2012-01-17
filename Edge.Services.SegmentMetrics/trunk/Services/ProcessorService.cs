using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Services.SegmentMetrics;
using Newtonsoft.Json;

namespace Edge.Services.SegmentMetrics.Services
{
	public class ProcessorService : PipelineService
	{

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			
			foreach (var ReportFile in Delivery.Files)
			{
				var ReportReader = new XmlDynamicReader
					(ReportFile.OpenContents(), ReportFile.Parameters["Bo.Xpath"].ToString());
				Dictionary<string, double> totalsValidation = new Dictionary<string, double>();

				using (var session = new SegmentMetricsImportManager(this.Instance.InstanceID))
				{
					session.BeginImport(this.Delivery);
					#region For Validation
					foreach (var measure in session.Measures.Values)
					{
						if (measure.Options.HasFlag(MeasureOptions.ValidationRequired))
						{
							if (!totalsValidation.ContainsKey(measure.SourceName))
								totalsValidation.Add(measure.SourceName, 0); //TODO : SHOULD BE NULL BUT SINCE CAN'T ADD NULLABLE ...TEMP

						}


					}
					#endregion
					using (ReportReader)
					{

						while (ReportReader.Read())
						{

							SegmentMetricsUnit boMetricsUnit = new SegmentMetricsUnit();
							//TODO: Validations
							boMetricsUnit.Segments[Segment.TrackerSegment] = new SegmentValue() { Value = ReportReader.Current.Attributes.ID };
							foreach (var measure in session.Measures.Values)
							{

								if (totalsValidation.ContainsKey(measure.SourceName))
									totalsValidation[measure.SourceName] += Convert.ToDouble(ReportReader.Current.Attributes[measure.SourceName]);
								boMetricsUnit.MeasureValues[session.Measures[measure.Name]] = Convert.ToDouble(ReportReader.Current.Attributes[measure.SourceName]);



							}

							//Create Usid
							Dictionary<string, string> usid = new Dictionary<string, string>();
							foreach (var segment in boMetricsUnit.Segments)
							{
								usid.Add(segment.Key.Name, segment.Value.Value);
							}

							boMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
							boMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();
							session.ImportMetrics(boMetricsUnit, JsonConvert.SerializeObject(usid));
						}
					}
					session.HistoryEntryParameters.Add(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, totalsValidation);
					session.EndImport(); 
				}
			}
			return Core.Services.ServiceOutcome.Success;
		}
	}
}
