using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using GA = Google.Api.Ads.AdWords.v201101;
using Edge.Core.Services;
using Edge.Services.AdMetrics;
using Edge.Core.Configuration;

namespace Edge.Services.Google.AdWords
{
	public class InitializerService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			this.Delivery = this.NewDelivery(); // setup delivery

			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["KeywordContentId"]))
				throw new Exception("Missing Configuration Param , KeywordContentId");
			
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["Adwords.MccEmail"]))
				throw new Exception("Missing Configuration Param , Adwords.MccEmail");

			if (String.IsNullOrEmpty (this.Instance.Configuration.Options["Adwords.ClientID"]))
				throw new Exception("Missing Configuration Param , Adwords.ClientID");

			//checking for conflicts 
			this.Delivery.Signature =Delivery.CreateSignature( String.Format("GoogleAdwordsSearch-[{0}]-[{1}]-[{2}]-[{3}]",//EdgeAccountID , MCC Email ,AdwordsClientID , TargetPeriod
				this.Instance.AccountID,
				this.Instance.Configuration.Options["Adwords.MccEmail"].ToString(),
				this.Instance.Configuration.Options["Adwords.ClientID"].ToString(),
				this.TargetPeriod.ToAbsolute()));


			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new AdMetricsImportManager.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlRollbackCommand]
			});

			// will use ConflictBehavior configuration option to abort or rollback if any conflicts occur
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			
			this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];
			if (string.IsNullOrEmpty(this.Delivery.TargetLocationDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");
			this.Delivery.TargetPeriod = this.TargetPeriod;
            this.Delivery.Account = new Edge.Data.Objects.Account() { ID = this.Instance.AccountID, OriginalID = this.Instance.Configuration.Options["Adwords.ClientID"] };
			this.Delivery.Channel = new Data.Objects.Channel() { ID = 1 };

			#region Must Have Params

			//Get MCC Email
			
			this.Delivery.Parameters["MccEmail"] = this.Instance.Configuration.Options["Adwords.MccEmail"];
			this.Delivery.Parameters["KeywordContentId"] = this.Instance.Configuration.Options["KeywordContentId"];

			// Get Report types
			string[] reportTypeNames = this.Instance.Configuration.Options["Adwords.ReportType"].Split('|');
			List<GA.ReportDefinitionReportType> reportTypes = new List<GA.ReportDefinitionReportType>();
			foreach (string reportTypeName in reportTypeNames)
			{
				if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
					reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
				else throw new Exception("Undefined ReportType");
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
			this.Delivery.Parameters["includeZeroImpression"] = false;

			//Check for includeConversionTypes
			string includeConversionTypes;
			if (!String.IsNullOrEmpty(includeConversionTypes = Instance.Configuration.Options["includeConversionTypes"]))
			{
				this.Delivery.Parameters["includeConversionTypes"] = includeConversionTypes;
			}
			this.Delivery.Parameters["includeConversionTypes"] = false; // deafult

			//Check for includeDisplaytData
			string includeDisplaytData;
			if (!String.IsNullOrEmpty(includeDisplaytData = Instance.Configuration.Options["includeDisplaytData"]))
			{
				this.Delivery.Parameters["includeDisplaytData"] = includeDisplaytData;
			}
			this.Delivery.Parameters["includeDisplaytData"] = false; // deafult

			#endregion

			//Creating Delivery files Per Client ID 
			foreach (string clientId in adwordsClientIds)
			{
				foreach (GA.ReportDefinitionReportType reportType in (List<GA.ReportDefinitionReportType>)this.Delivery.Parameters["reportTypes"])
				{
					DeliveryFile file = new DeliveryFile();
					file.Name = GoogleStaticReportsNamesUtill._reportNames[reportType];
					//file.Name = reportType.ToString();
					file.Parameters.Add("AdwordsClientID", clientId);
					this.Delivery.Files.Add(file);
				}
				if (Boolean.Parse(includeConversionTypes)) // if AD Performance With conversion type is required 
				{
					DeliveryFile file = new DeliveryFile();
					file.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv";
					//file.Name = "AD_PERFORMANCE_REPORT_(Conversion)";
					file.Parameters.Add("AdwordsClientID", clientId);
					this.Delivery.Files.Add(file);
				}
				if (Boolean.Parse(includeDisplaytData)) // if AD Performance With conversion type is required 
				{
					DeliveryFile file = new DeliveryFile();
					file.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.MANAGED_PLACEMENTS_PERFORMANCE_REPORT];
					//file.Name = "MANAGED_PLACEMENTS_PERFORMANCE_REPORT";
					file.Parameters.Add("AdwordsClientID", clientId);
					this.Delivery.Files.Add(file);
				}

				//...............................
				//AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT
				//{
				//    DeliveryFile file = new DeliveryFile();
				//    file.Name = GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT];
				//    file.Parameters.Add("Email", email);
				//    this.Delivery.Files.Add(file);
				//}
				//...............................
			}
			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;
		}

	}
}
