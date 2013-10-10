using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using GA = Google.Api.Ads.AdWords.v201309;
using Edge.Core.Services;
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Metrics;
using Edge.Core.Configuration;
using Google.Api.Ads.AdWords.v201309;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util.Reports;
using Edge.Data.Pipeline.Common.Importing;

namespace Edge.Services.Google.AdWords
{
    public class InitializerService : PipelineService
    {
        protected override ServiceOutcome DoPipelineWork()
        {
            this.Delivery = this.NewDelivery(); // setup delivery


            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["FilterDeleted"]))
                throw new Exception("Missing Configuration Param , FilterDeleted");
            this.Delivery.Parameters["FilterDeleted"] = this.Instance.Configuration.Options["FilterDeleted"];

            if (this.Instance.Configuration.Options.ContainsKey("AppendSitelinks"))
                this.Delivery.Parameters.Add("AppendSitelinks", this.Instance.Configuration.Options["AppendSitelinks"]);

            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["KeywordContentId"]))
                throw new Exception("Missing Configuration Param , KeywordContentId");

            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["Adwords.MccEmail"]))
                throw new Exception("Missing Configuration Param , Adwords.MccEmail");

            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["Adwords.ClientID"]))
                throw new Exception("Missing Configuration Param , Adwords.ClientID");


            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["SubChannelName"]))
                throw new Exception("Missing Configuration Param , SubChannelName");


            //checking for conflicts 
            this.Delivery.Outputs.Add(new DeliveryOutput()
            {
                Signature = Delivery.CreateSignature(String.Format("{4}-[{0}]-[{1}]-[{2}]-[{3}]",//-[{4}]",//EdgeAccountID , MCC Email ,AdwordsClientID , TimePeriod
                    this.Instance.AccountID,
                    this.Instance.Configuration.Options["Adwords.MccEmail"].ToString(),
                    this.Instance.Configuration.Options["Adwords.ClientID"].ToString(),
                    this.TimePeriod.ToAbsolute(),
                    this.Instance.Configuration.Options["SubChannelName"].ToString()
                )),
                Account = new Data.Objects.Account() { ID = this.Instance.AccountID, OriginalID = this.Instance.Configuration.Options["Adwords.ClientID"] },
                Channel = new Data.Objects.Channel() { ID = 1 },
                TimePeriodStart = Delivery.TimePeriodStart,
                TimePeriodEnd = Delivery.TimePeriodEnd
            }
            );

            this.Delivery.FileDirectory = Instance.Configuration.Options[Edge.Data.Pipeline.Services.Const.DeliveryServiceConfigurationOptions.FileDirectory];
            if (string.IsNullOrEmpty(this.Delivery.FileDirectory))
                throw new Exception("Delivery FileDirectory must be configured in configuration file (DeliveryFilesDir)");
            this.Delivery.TimePeriodDefinition = this.TimePeriod;
            this.Delivery.Account = new Edge.Data.Objects.Account() { ID = this.Instance.AccountID, OriginalID = this.Instance.Configuration.Options["Adwords.ClientID"] };
            this.Delivery.Channel = new Data.Objects.Channel() { ID = 1 };


            // Create an import manager that will handle rollback, if necessary
            AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
            {
                SqlRollbackCommand = Instance.Configuration.Options[Consts.AppSettings.SqlRollbackCommand]
            });

            // will use ConflictBehavior configuration option to abort or rollback if any conflicts occur
            this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

            #region Must Have Params
            this.Delivery.Parameters["IncludeStatus"] = this.Instance.Configuration.Options["IncludeStatus"];


            //Get MCC Paramerters
            this.Delivery.Parameters["DeveloperToken"] = this.Instance.Configuration.Options["DeveloperToken"];
            this.Delivery.Parameters["MccEmail"] = this.Instance.Configuration.Options["Adwords.MccEmail"];
            //this.Delivery.Parameters["MccPass"] = Core.Utilities.Encryptor.Dec(this.Instance.Configuration.Options["Adwords.MccPass"].ToString());
            this.Delivery.Parameters["KeywordContentId"] = this.Instance.Configuration.Options["KeywordContentId"];

            // Get Report types
            string[] reportTypeNames = this.Instance.Configuration.Options["Adwords.ReportType"].Split('|');
            List<GA.ReportDefinitionReportType> reportTypes = new List<GA.ReportDefinitionReportType>();
            foreach (string reportTypeName in reportTypeNames)
            {
                if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
                    reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
                else throw new Exception("Undefined Google Adwords ReportType");
            }
            this.Delivery.Parameters["reportTypes"] = reportTypes;

            //Get Account Client Id's
            string[] adwordsClientIds = this.Instance.Configuration.Options["Adwords.ClientID"].Split('|');
            this.Delivery.Parameters["AdwordsClientIDs"] = adwordsClientIds;

            #endregion

            #region Nice to have params

            //Check for includeZeroImpression
            string includeZeroImpression;
            if (!String.IsNullOrEmpty(includeZeroImpression = Instance.Configuration.Options["includeZeroImpression"]))
            {
                this.Delivery.Parameters["includeZeroImpression"] = includeZeroImpression;
            }
            else
                this.Delivery.Parameters["includeZeroImpression"] = false;

            //Check for includeConversionTypes
            string includeConversionTypes;
            if (!String.IsNullOrEmpty(includeConversionTypes = Instance.Configuration.Options["includeConversionTypes"]))
            {
                this.Delivery.Parameters["includeConversionTypes"] = includeConversionTypes;
            }
            else
                this.Delivery.Parameters["includeConversionTypes"] = false; // deafult

            //Check for includeDisplaytData
            string includeDisplaytData;
            if (!String.IsNullOrEmpty(includeDisplaytData = Instance.Configuration.Options["includeDisplaytData"]))
            {
                this.Delivery.Parameters["includeDisplaytData"] = includeDisplaytData;
            }
            else
                this.Delivery.Parameters["includeDisplaytData"] = false; // deafult

            #endregion

            //Creating Delivery files Per Client ID 
            foreach (string clientId in adwordsClientIds)
            {
                foreach (GA.ReportDefinitionReportType reportType in (List<GA.ReportDefinitionReportType>)this.Delivery.Parameters["reportTypes"])
                {
                    DeliveryFile file = new DeliveryFile();
                    file.Name = GoogleStaticReportsNamesUtill._reportNames[reportType];
                    file.Parameters.Add("ReportType", reportType.ToString());
                    file.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.DEFAULT);
                    file.Parameters.Add("AdwordsClientID", clientId);
                    this.Delivery.Files.Add(file);


                    //Handelling conversion
                    if (Boolean.Parse(this.Delivery.Parameters["includeConversionTypes"].ToString())) // if AD Performance With conversion type is required 
                    {
                        bool addFile = true;
                        DeliveryFile conversionFile = new DeliveryFile();
                        conversionFile.Name = GoogleStaticReportsNamesUtill._reportNames[reportType] + "_Conv";
                        switch (reportType)
                        {
                            case GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT:
                                conversionFile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT.ToString());
                                break;

                            case GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT:
                                conversionFile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT.ToString());
                                break;
                            default: addFile = false; break;
                        }

                        if (addFile)
                        {
                            conversionFile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.CONVERSION);
                            conversionFile.Parameters.Add("AdwordsClientID", clientId);
                            this.Delivery.Files.Add(conversionFile);
                        }

                    }

                    //Add Status Reports
                    DeliveryFile AD_Camp_Adgroups_StatusFile = new DeliveryFile();
                    AD_Camp_Adgroups_StatusFile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT.ToString());

                }

                if (Boolean.Parse(this.Delivery.Parameters["includeDisplaytData"].ToString())) // if AD Performance With conversion type is required 
                {
                    DeliveryFile file = new DeliveryFile();
                    file.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT];
                    file.Parameters.Add("ReportType", GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT.ToString());
                    file.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.DEFAULT);
                    file.Parameters.Add("AdwordsClientID", clientId);
                    this.Delivery.Files.Add(file);
                }

                #region Sitelinks

                if (this.Delivery.Parameters.ContainsKey("AppendSitelinks"))
                    if (Boolean.Parse(this.Delivery.Parameters["AppendSitelinks"].ToString()))
                    {
                        DeliveryFile siteLinkFile = new DeliveryFile();
                        siteLinkFile.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEHOLDER_FEED_ITEM_REPORT];
                        siteLinkFile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.PLACEHOLDER_FEED_ITEM_REPORT.ToString());
                        siteLinkFile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.DEFAULT);
                        siteLinkFile.Parameters.Add("AdwordsClientID", clientId);
                        this.Delivery.Files.Add(siteLinkFile);
                    }

                #endregion


                #region Status Reports
                if (Boolean.Parse(this.Delivery.Parameters["IncludeStatus"].ToString())) // if AD Performance With conversion type is required 
                {
                    //1. create file for **** Ad performance with status
                    DeliveryFile adPerformaceStatusfile = new DeliveryFile();
                    adPerformaceStatusfile.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Status";
                    adPerformaceStatusfile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT.ToString());
                    adPerformaceStatusfile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.STATUS);
                    adPerformaceStatusfile.Parameters.Add("AdwordsClientID", clientId);
                    this.Delivery.Files.Add(adPerformaceStatusfile);

                    //2. create file for **** KWD performance with status
                    DeliveryFile kwdPerformaceStatusfile = new DeliveryFile();
                    kwdPerformaceStatusfile.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT] + "_Status";
                    kwdPerformaceStatusfile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT.ToString());
                    kwdPerformaceStatusfile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.STATUS);
                    kwdPerformaceStatusfile.Parameters.Add("AdwordsClientID", clientId);
                    this.Delivery.Files.Add(kwdPerformaceStatusfile);

                    //3. create file for **** Managed performance with status
                    DeliveryFile ManagedGDNStatusfile = new DeliveryFile();
                    ManagedGDNStatusfile.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT] + "_Status";
                    ManagedGDNStatusfile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT.ToString());
                    ManagedGDNStatusfile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.STATUS);
                    ManagedGDNStatusfile.Parameters.Add("AdwordsClientID", clientId);
                    this.Delivery.Files.Add(ManagedGDNStatusfile);
                }
                #endregion

            }
            this.Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }

    }
}
