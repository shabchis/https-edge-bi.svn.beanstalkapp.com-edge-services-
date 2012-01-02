using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Services.SegmentMetrics;
using Newtonsoft.Json;

namespace Edge.Services.BackOffice.Cunduit
{
	public class BoProcessorService : PipelineService
	{
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			DeliveryFile boReportFile = Delivery.Files[BoConfigurationOptions.BoFileName];
			var boReportReader = new XmlDynamicReader
			(boReportFile.OpenContents(), "OnlineMarketingReport/Trackers/Tracker");
			Dictionary<string, double> totalsValidation = new Dictionary<string, double>();

			using (var session = new SegmentMetricsImportManager(this.Instance.InstanceID))
			{
				session.BeginImport(this.Delivery);
				#region For Validation
				foreach (var measure in session.Measures)
				{
				    if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
				    {
				        if (!totalsValidation.ContainsKey(measure.Key))
				            totalsValidation.Add(measure.Key, 0); //TODO : SHOULD BE NULL BUT SINCE CAN'T ADD NULLABLE ...TEMP

				    }


				}
				#endregion
				using (boReportReader)
				{
					
					while (boReportReader.Read())
					{

						SegmentMetricsUnit boMetricsUnit = new SegmentMetricsUnit();
						//TODO: Validations
						boMetricsUnit.Segments[Segment.TrackerSegment] = new SegmentValue() { Value = boReportReader.Current.Attributes.ID };

						totalsValidation[BoFields.Signups] = Convert.ToInt32(boReportReader.Current.Attributes.Signups);
						boMetricsUnit.MeasureValues[session.Measures[BoFields.Signups]] = Convert.ToInt32(boReportReader.Current.Attributes.Signups);

						totalsValidation[BoFields.GoodToolbars] = Convert.ToInt32(boReportReader.Current.Attributes.GoodToolbars);
						boMetricsUnit.MeasureValues[session.Measures[BoFields.GoodToolbars]] = Convert.ToDouble(boReportReader.Current.Attributes.GoodToolbars);

						totalsValidation[BoFields.GreatToolbars] = Convert.ToInt32(boReportReader.Current.Attributes.GreatToolbars);
						boMetricsUnit.MeasureValues[session.Measures[BoFields.GreatToolbars]] = Convert.ToDouble(boReportReader.Current.Attributes.GreatToolbars);

						totalsValidation[BoFields.TotalInstalls] = Convert.ToInt32(boReportReader.Current.Attributes.TotalInstalls);
						boMetricsUnit.MeasureValues[session.Measures[BoFields.TotalInstalls]] = Convert.ToDouble(boReportReader.Current.Attributes.TotalInstalls);

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
			return Core.Services.ServiceOutcome.Success;
		}
	}
}
