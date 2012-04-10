using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Newtonsoft.Json;

namespace Edge.Services.GenericMetrics.Services
{
	
    public class ProcessorService : PipelineService
    {

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            foreach (var ReportFile in Delivery.Files)
            {
                bool isAttribute = Boolean.Parse(ReportFile.Parameters["Bo.IsAttribute"].ToString());
                var ReportReader = new XmlDynamicReader
                    (ReportFile.OpenContents(), ReportFile.Parameters["Bo.Xpath"].ToString());
                Dictionary<string, double> totalsValidation = new Dictionary<string, double>();

				using (var session = new GenericMetricsImportManager(this.Instance.InstanceID)
				{
					MeasureOptions = MeasureOptions.IsBackOffice,
					MeasureOptionsOperator = OptionsOperator.And,
					SegmentOptions = Data.Objects.SegmentOptions.All,
					SegmentOptionsOperator = OptionsOperator.And
				})
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
                        dynamic readerHelper;


                        while (ReportReader.Read())
                        {
                            if (isAttribute)
                                readerHelper = ReportReader.Current.Attributes;
                            else
                                readerHelper = ReportReader.Current;
							
							//Setting MetricsUnit
							GenericMetricsUnit MetricsUnit = new GenericMetricsUnit();
							MetricsUnit.SegmentDimensions = new Dictionary<Segment, SegmentObject>();
							
							//Ask Doron about not having targets 
							MetricsUnit.TargetDimensions = new List<Target>();

							MetricsUnit.MeasureValues = new Dictionary<Measure, double>();

							MetricsUnit.Account = new Account()
							{
								ID = this.Delivery.Account.ID
							};

							MetricsUnit.Channel = new Channel()
							{
								ID = -1
							};

							//Getting Tracker
							SegmentObject tracker = new SegmentObject()
							{
								Value = readerHelper[ReportFile.Parameters["Bo.TrackerIDField"].ToString()]
							};
							
							MetricsUnit.SegmentDimensions.Add(session.SegmentTypes[Segment.Common.Tracker], tracker);

						    
							//Getting Measures
							foreach (var measure in session.Measures.Values)
                            {
                                if (string.IsNullOrEmpty(measure.SourceName)&& measure.Account != null)
                                    throw new Exception(string.Format("Undifined Source Name in DB for measure {0} ", measure.Name));

                                if (measure.Account != null)
                                {
                                    if (!string.IsNullOrEmpty(readerHelper[measure.SourceName]))                                   
                                        MetricsUnit.MeasureValues[session.Measures[measure.Name]] = Convert.ToDouble(readerHelper[measure.SourceName]);

                                    if (totalsValidation.ContainsKey(measure.SourceName))
                                    {
                                        if (!string.IsNullOrEmpty(readerHelper[measure.SourceName]))
                                            totalsValidation[measure.SourceName] += Convert.ToDouble(readerHelper[measure.SourceName]);
                                    }
                                }

                            }

                            //Create Usid
                            Dictionary<string, string> usid = new Dictionary<string, string>();
                            foreach (var segment in MetricsUnit.SegmentDimensions)
                            {
                                usid.Add(segment.Key.Name, segment.Value.Value);
                            }

                            MetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
                            MetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

							session.ImportMetrics(MetricsUnit);
                            //session.ImportMetrics(MetricsUnit, JsonConvert.SerializeObject(usid));
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
