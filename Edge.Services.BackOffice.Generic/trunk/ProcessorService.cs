﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
//using Edge.Services.SegmentMetrics;
using Newtonsoft.Json;
using Edge.Data.Pipeline.Mapping;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Metrics.GenericMetrics;
using Edge.Data.Pipeline.Common.Importing;

namespace Edge.Services.BackOffice.Generic
{
	public class ProcessorService : MetricsProcessorServiceBase
    {
		DeliveryOutput currentOutput;

		public new GenericMetricsImportManager ImportManager
		{
			get { return (GenericMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
			MappingContainer metricsUnitMapping = this.Mappings.Objects[typeof(GenericMetricsUnit)];
			currentOutput = this.Delivery.Outputs.First();
			currentOutput.Checksum = new Dictionary<string, double>();

            foreach (var ReportFile in Delivery.Files)
            {
                bool isAttribute = Boolean.Parse(ReportFile.Parameters["Bo.IsAttribute"].ToString());
                var ReportReader = new XmlDynamicReader
                    (ReportFile.OpenContents(), ReportFile.Parameters["Bo.Xpath"].ToString());
                Dictionary<string, double> totalsValidation = new Dictionary<string, double>();



				using (this.ImportManager = new GenericMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
				{
					MeasureOptions = MeasureOptions.IsBackOffice,
					MeasureOptionsOperator = OptionsOperator.Or,
					SegmentOptions = Data.Objects.SegmentOptions.All,
					SegmentOptionsOperator = OptionsOperator.And
				}))
				{
					ImportManager.BeginImport(this.Delivery);
					
                    using (ReportReader)
                    {
                        dynamic readerHelper;
						


                        while (ReportReader.Read())
                        {
                            if (isAttribute)
                                readerHelper = ReportReader.Current.Attributes;
                            else
                                readerHelper = ReportReader.Current;

							this.Mappings.OnFieldRequired = field => readerHelper[field];
							GenericMetricsUnit genericMetricsUnit = new GenericMetricsUnit();
							if (this.Mappings.Objects.ContainsKey(typeof(GenericMetricsUnit))
								throw new Exception("Mappings object not contains object genericMetricsUnit");
							metricsUnitMapping.Apply(genericMetricsUnit);

							foreach (var m in genericMetricsUnit.MeasureValues)
							{
								if (m.Key.Options.HasFlag(MeasureOptions.ValidationRequired))
									if (!currentOutput.Checksum.ContainsKey(m.Key.Name))
										currentOutput.Checksum.Add(m.Key.Name, m.Value);
									else
										currentOutput.Checksum[m.Key.Name] += m.Value;
							}
							genericMetricsUnit.Output = currentOutput;
							ImportManager.ImportMetrics(genericMetricsUnit);
                           

                           
                        }
                    }
                   
					ImportManager.EndImport();
                }
            }
            return Core.Services.ServiceOutcome.Success;
        }
    }
}
