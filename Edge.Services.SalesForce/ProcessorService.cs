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
using System.Globalization;

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

        //Developed by Alon for Green SQL.
        protected int GetConversion(object d)
        {
            int result = 0;
            DateTime date;
            string dd = System.Convert.ToString(d);
            if (!string.IsNullOrEmpty(dd))
            {
                date = DateTime.Parse(dd, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
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
                foreach (var ReportFile in Delivery.Files)
                {
                    //check number of recordes
                    JsonDynamicReader reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.None), "$.totalSize");
                    int numOfRecordes = 0;
                    if (reportReader.Read())
                        numOfRecordes = int.Parse(reportReader.Current.totalSize);

                    reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.None), "$.nextRecordsUrl");
                    if (reportReader.Read())
                    {
                        Log.Write(string.Format("Salesforce Attention - account {0} contains more than 1 file per query {1}", this.Delivery.Account.ID, ReportFile.Parameters["Query"].ToString()),LogMessageType.Warning);
                    }

                    if (numOfRecordes > 0)
                    {
                        //Get Values
                        reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.None), "$.records[*].*");
                        using (reportReader)
                        {
                            this.Mappings.OnFieldRequired = field =>
                                {
                                    object value = new object();
                                    string[] nestedFields;

                                    try
                                    {
                                        if (field.Contains('.'))
                                        {
                                            nestedFields = field.Split('.');
                                            value = reportReader.Current[nestedFields[0]];
                                            foreach (var item in nestedFields)
                                            {
                                              if(item.Equals(nestedFields[0])) continue;
                                              value = ((Dictionary<string,object>)value)[item];
                                            }
                                            value = reportReader.Current[ nestedFields[0]][nestedFields[1]];
                                            Console.Write(value.ToString());
                                            return value;
                                        }
                                        else
                                        {
                                            Console.Write(value.ToString());
                                            return reportReader.Current[field];
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new Exception(string.Format("Error while trying to map field {0} from mapper", field), ex);
                                    }

                                    
                                };
                        

                            while (reportReader.Read())
                            {
                                GenericMetricsUnit metricsUnit = new GenericMetricsUnit();
                                metricsUnitMapping.Apply(metricsUnit);
                                if (metricsUnit.MeasureValues != null)
                                {
                                    SegmentObject tracker = metricsUnit.SegmentDimensions[ImportManager.SegmentTypes[Segment.Common.Tracker]];
                                    GenericMetricsUnit importedUnit = null;

                                    // check if we already found a metrics unit with the same tracker
                                    if (!data.TryGetValue(tracker.Value, out importedUnit))
                                    {
                                        metricsUnit.Output = currentOutput;
                                        data.Add(tracker.Value, metricsUnit);
                                    }
                                    else //Tracker already exists
                                    {
                                        // Merge captured measure with existing measures
                                        foreach (var capturedMeasure in metricsUnit.MeasureValues)
                                        {
                                            if (!capturedMeasure.Key.Options.HasFlag(MeasureOptions.IsBackOffice))
                                                continue;
                                            //Measure already exists per tracker than aggregate:
                                            if (importedUnit.MeasureValues.ContainsKey(capturedMeasure.Key))
                                                importedUnit.MeasureValues[capturedMeasure.Key] += capturedMeasure.Value;
                                            else
                                                //Captured Measure doest exists with this tracker:
                                                importedUnit.MeasureValues.Add(capturedMeasure.Key, capturedMeasure.Value);
                                        }
                                    }

                                    #region Validation
                                    // For validations
                                    foreach (var m in metricsUnit.MeasureValues)
                                    {
                                        if (m.Key.Options.HasFlag(MeasureOptions.ValidationRequired))
                                        {
                                            if (!currentOutput.Checksum.ContainsKey(m.Key.Name))
                                                currentOutput.Checksum.Add(m.Key.Name, m.Value);
                                            else
                                                currentOutput.Checksum[m.Key.Name] += m.Value;
                                        }
                                    }
                                    #endregion

                                }
                            }
                        }

                    }
                    else
                        Log.Write("No Records Found in File " + ReportFile.Name, LogMessageType.Information);

                }
                // Import all unique units per tracker
                foreach (GenericMetricsUnit metricsUnit in data.Values)
                {
                    this.ImportManager.ImportMetrics(metricsUnit);
                }

                ImportManager.EndImport();
            }
            return Core.Services.ServiceOutcome.Success;
        }
    }
}
