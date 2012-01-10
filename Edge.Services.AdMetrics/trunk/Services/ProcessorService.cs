using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Data;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
//using Edge.Data.Pipeline.ImportMapping;
using Edge.Services.SegmentMetrics;
using Newtonsoft.Json;

namespace Edge.Services.AdMetrics
{
	//public class XmlProcessorService : PipelineService
	//{
	//    protected override Core.Services.ServiceOutcome DoPipelineWork()
	//    { 
	//        // TODO: setup/defaults/configuration/etc.
	//        // ------------------------------------------


	//        Dictionary<string, double> totalsValidation = new Dictionary<string, double>();
	//        DeliveryFile file = this.Delivery.Files["BO.xml"];
	//        if (file == null)
	//            throw new Exception(String.Format("Could not find delivery file '{0}' in the delivery.", "BO.xml"));

	//        //FileCompression compression;
	//        //string compressionOption;
	//        //if (this.Instance.Configuration.Options.TryGetValue(Const.ConfigurationOptions.Compression, out compressionOption))
	//        //{
	//        //    if (!Enum.TryParse<FileCompression>(compressionOption, out compression))
	//        //        throw new ConfigurationException(String.Format("Invalid compression type '{0}'.", compressionOption));
	//        //}
	//        //else
	//        //    compression = FileCompression.None;


	//        string xpath = file.Parameters["Bo.Xpath"].ToString(); // TODO: 

			
			

	//        // ------------------------------------------

			

	//        using (var stream = file.OpenContents())
	//        {
	//            using (XmlDynamicReader reader = new XmlDynamicReader(stream, xpath))
	//            {
	//                Func<string, string> readFunction = requestedField => 
	//                {
	//                    if (requestedField.StartsWith("@"))
	//                        return reader.Current.Attributes[requestedField];
	//                    else
	//                        return reader.Current[requestedField];
	//                };

	//                using (var session = new SegmentMetricsImportManager(this.Instance.InstanceID))
	//                {

						
	//                    session.BeginImport(this.Delivery);
	//                    MappingConfiguration mappingConfig = MappingConfiguration.Load(@"C:\ConduitMappings.xml",session.Measures,session.Segments);
	//                    MappedObject segmentMetricsUnitMapping = mappingConfig.Objects[typeof(SegmentMetricsUnit)];
	//                    #region For Validation
	//                    foreach (var measure in session.Measures)
	//                    {
	//                        if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
	//                        {
	//                            if (!totalsValidation.ContainsKey(measure.Key))
	//                                totalsValidation.Add(measure.Key, 0); //TODO : SHOULD BE NULL BUT SINCE CAN'T ADD NULLABLE ...TEMP

	//                        }


	//                    }
	//                    #endregion
						
	//                    while (reader.Read())
	//                    {

	//                        var boMetricsUnit = new SegmentMetricsUnit();
	//                        boMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
	//                        boMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();
	//                        segmentMetricsUnitMapping.Apply(boMetricsUnit, readFunction);
	//                        //Create Usid
	//                        Dictionary<string, string> usid = new Dictionary<string, string>();
	//                        foreach (var segment in boMetricsUnit.Segments)
	//                        {
	//                            usid.Add(segment.Key.Name, segment.Value.Value);
	//                        }
							

							

	//                        session.ImportMetrics(boMetricsUnit, JsonConvert.SerializeObject(usid));
	//                    }

	//                    session.EndImport();
	//                }
	//            }
	//        }

	//        return Core.Services.ServiceOutcome.Success;
	//    }

	//}
}
