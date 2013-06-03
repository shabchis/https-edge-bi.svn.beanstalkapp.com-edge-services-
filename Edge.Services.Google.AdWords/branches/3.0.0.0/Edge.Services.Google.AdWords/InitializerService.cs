using System;
using System.Collections.Generic;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Misc;
using Edge.Data.Pipeline.Services;
using GA = Google.Api.Ads.AdWords.v201302;
using Edge.Core.Services;

namespace Edge.Services.Google.AdWords
{
	public class InitializerService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			if (!Configuration.Parameters.ContainsKey("FilterDeleted"))
				throw new Exception("Missing Configuration Param: FilterDeleted");

			if (!Configuration.Parameters.ContainsKey("KeywordContentId"))
				throw new Exception("Missing Configuration Param: KeywordContentId");

			if (!Configuration.Parameters.ContainsKey("Adwords.MccEmail"))
				throw new Exception("Missing Configuration Param: Adwords.MccEmail");

			if (!Configuration.Parameters.ContainsKey("Adwords.ClientID"))
				throw new Exception("Missing Configuration Param: Adwords.ClientID");

			if (!Configuration.Parameters.ContainsKey("SubChannelName"))
				throw new Exception("Missing Configuration Param: SubChannelName"); 
			
			if (!Configuration.TimePeriod.HasValue)
				throw new Exception("No time period is set for Service"); 

			var accountId = Configuration.Parameters.Get("AccountID", false, -1);
			var channelId = Configuration.Parameters.Get("ChannelID", false, -1);

			if (Delivery == null)
			{
				Delivery = NewDelivery();
				Delivery.TimePeriodDefinition = Configuration.TimePeriod.Value;
				Delivery.Account = accountId != -1 ? new Account {ID = accountId} : null;
				Delivery.FileDirectory = Configuration.Parameters.Get<string>(Const.DeliveryServiceConfigurationOptions.FileDirectory);
				if (channelId != -1) Delivery.Channel = new Channel {ID = channelId};
			}
			Delivery.Parameters["FilterDeleted"] = Configuration.Parameters["FilterDeleted"];

			//checking for conflicts 
			var dateRanges = Delivery.TimePeriodDefinition.ToAbsoluteSplit(DateTimeRangeSplitResolution.Day);
			foreach (var dateRange in dateRanges)
			{
				Delivery.Outputs.Add(new DeliveryOutput
					{
						Signature = Delivery.CreateSignature(String.Format("{4}-[{0}]-[{1}]-[{2}]-[{3}]",
																		   accountId,
																		   Configuration.Parameters["Adwords.MccEmail"],
																		   Configuration.Parameters["Adwords.ClientID"],
																		   dateRange.ToAbsolute(),
																		   Configuration.Parameters["SubChannelName"])),
						Account = new Account {ID = accountId},
						Channel = new Channel {ID = channelId},
						TimePeriodStart = dateRange.Start.BaseDateTime,
						TimePeriodEnd = dateRange.Start.BaseDateTime
					}
					);
			}

			// Create an import manager that will handle rollback, if necessary
			var importManager = new MetricsDeliveryManager(InstanceID, null, new MetricsDeliveryManagerOptions
			{
				SqlRollbackCommand = Configuration.Parameters[Consts.AppSettings.SqlRollbackCommand].ToString()
			});

			// will use ConflictBehavior configuration option to abort or rollback if any conflicts occur
			HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			#region Must Have Params
			Delivery.Parameters["IncludeStatus"] = Configuration.Parameters.Get<string>("IncludeStatus");

			//Get MCC Paramerters
			Delivery.Parameters["DeveloperToken"] = Configuration.Parameters.Get<string>("DeveloperToken");
			Delivery.Parameters["MccEmail"] = Configuration.Parameters.Get<string>("Adwords.MccEmail");
			//this.Delivery.Parameters["MccPass"] = Core.Utilities.Encryptor.Dec(this.Instance.Configuration.Options["Adwords.MccPass"].ToString());
			Delivery.Parameters["KeywordContentId"] = Configuration.Parameters.Get<string>("KeywordContentId");

			// Get Report types
			string[] reportTypeNames = Configuration.Parameters.Get<string>("Adwords.ReportType").Split('|');
			var reportTypes = new List<GA.ReportDefinitionReportType>();
			foreach (var reportTypeName in reportTypeNames)
			{
				if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
					reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
				else throw new Exception("Undefined Google Adwords ReportType");
			}
			Delivery.Parameters["reportTypes"] = reportTypes;

			//Get Account Client Id's
			string[] adwordsClientIds = Configuration.Parameters.Get<string>("Adwords.ClientID").Split('|');
			Delivery.Parameters["AdwordsClientIDs"] = adwordsClientIds;

			#endregion

			#region Nice to have params

			//Check for includeZeroImpression
			if (Configuration.Parameters.ContainsKey("includeZeroImpression"))
				Delivery.Parameters["includeZeroImpression"] = Configuration.Parameters.Get<string>("includeZeroImpression");
			else
				Delivery.Parameters["includeZeroImpression"] = false;

			//Check for includeConversionTypes
			if (Configuration.Parameters.ContainsKey("includeConversionTypes"))
				Delivery.Parameters["includeConversionTypes"] = Configuration.Parameters.Get<string>("includeConversionTypes");
			else
				Delivery.Parameters["includeConversionTypes"] = false; // default

			//Check for includeDisplaytData
			if (Configuration.Parameters.ContainsKey("includeDisplaytData"))
				Delivery.Parameters["includeDisplaytData"] = Configuration.Parameters.Get<string>("includeDisplaytData");
			else
				Delivery.Parameters["includeDisplaytData"] = false; // default
			#endregion

			//Creating Delivery files Per Client ID 
			foreach (var clientId in adwordsClientIds)
			{
				foreach (var reportType in Delivery.Parameters["reportTypes"] as List<GA.ReportDefinitionReportType>)
				{
					var file = new DeliveryFile {Name = GoogleStaticReportsNamesUtill.ReportNames[reportType]};
					file.Parameters.Add("ReportType", reportType.ToString());
					file.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.DEFAULT);
					file.Parameters.Add("AdwordsClientID", clientId);
					Delivery.Files.Add(file);

					//Handelling conversion
					if (Boolean.Parse(Delivery.Parameters["includeConversionTypes"].ToString())) // if AD Performance With conversion type is required 
					{
						bool addFile = true;
						var conversionFile = new DeliveryFile {Name = GoogleStaticReportsNamesUtill.ReportNames[reportType] + "_Conv"};
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
							Delivery.Files.Add(conversionFile);
						}
					}

					//Add Status Reports
					var adCampAdgroupsStatusFile = new DeliveryFile();
					adCampAdgroupsStatusFile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT.ToString());
				}
			
				if (Boolean.Parse(Delivery.Parameters["includeDisplaytData"].ToString())) // if AD Performance With conversion type is required 
				{
					var file = new DeliveryFile{ Name = GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT] };
					file.Parameters.Add("ReportType", GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT.ToString());
					file.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.DEFAULT);
					file.Parameters.Add("AdwordsClientID", clientId);
					Delivery.Files.Add(file);
				}

                #region Status Reports 
				if (Boolean.Parse(Delivery.Parameters["IncludeStatus"].ToString())) // if AD Performance With conversion type is required 
				{
					//1. create file for **** Ad performance with status
					var adPerformaceStatusfile = new DeliveryFile{Name = GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Status"};
					adPerformaceStatusfile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT.ToString());
					adPerformaceStatusfile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.STATUS);
					adPerformaceStatusfile.Parameters.Add("AdwordsClientID", clientId);
					Delivery.Files.Add(adPerformaceStatusfile);

					//2. create file for **** KWD performance with status
					var kwdPerformaceStatusfile = new DeliveryFile{Name = GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT] + "_Status"};
					kwdPerformaceStatusfile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT.ToString());
					kwdPerformaceStatusfile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.STATUS);
					kwdPerformaceStatusfile.Parameters.Add("AdwordsClientID", clientId);
					Delivery.Files.Add(kwdPerformaceStatusfile);

					//3. create file for **** Managed performance with status
					var managedGdnStatusFile = new DeliveryFile{Name = GoogleStaticReportsNamesUtill.ReportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT] + "_Status"};
					managedGdnStatusFile.Parameters.Add("ReportType", GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT.ToString());
					managedGdnStatusFile.Parameters.Add("ReportFieldsType", ReportDefinitionReportFieldsType.STATUS);
					managedGdnStatusFile.Parameters.Add("AdwordsClientID", clientId);
					Delivery.Files.Add(managedGdnStatusFile);
                }
                #endregion
            }
			Delivery.Save();
			return ServiceOutcome.Success;
		}
	}
}
