using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using GA = Google.Api.Ads.AdWords.v201101;
using Edge.Core.Services;
using Edge.Data.Pipeline.AdMetrics;

namespace Edge.Services.Google.Adwords
{
	public class InitializerService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			//TO DO : get information from configuration by using Instance instead of ParentInstane
			this.Delivery = this.NewDelivery(); // setup delivery

			//checking for conflicts 
			this.Delivery.Signature = String.Format("GoogleAdwordsSearch-[{0}]-[{1}]-[{2}]-[{3}]",//EdgeAccountID , MCC Email ,GoogleAccountEmail , TargetPeriod
				this.Instance.AccountID,
				this.Instance.Configuration.Options["Adwords.MccEmail"].ToString(),
				this.Instance.Configuration.Options["Adwords.Email"].ToString(),
				this.TargetPeriod.ToAbsolute());


			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new AdMetricsImportManager.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			
			//TO DO: get ConflictBehavior from configuration 
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			this.Delivery.TargetLocationDirectory = "AdwordsSearch";
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Account = new Edge.Data.Objects.Account() { ID = this.Instance.AccountID };
			this.Delivery.Channel = new Data.Objects.Channel() { ID = 1 };

			#region Must Have Params

			//Get MCC Email
			if (String.IsNullOrEmpty(this.Instance.ParentInstance.Configuration.Options["Adwords.MccEmail"]))
				throw new Exception("Missing Configuration Param , Adwords.MccEmail");
			else this.Delivery.Parameters["MccEmail"] = this.Instance.ParentInstance.Configuration.Options["Adwords.MccEmail"];

			// Get Report types
			string[] reportTypeNames = this.Instance.ParentInstance.Configuration.Options["Adwords.ReportType"].Split('|');
			List<GA.ReportDefinitionReportType> reportTypes = new List<GA.ReportDefinitionReportType>();
			foreach (string reportTypeName in reportTypeNames)
			{
				if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
					reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
				else throw new Exception("Undefined ReportType");
			}
			this.Delivery.Parameters["reportTypes"] = reportTypes;

			//Get Account Emails
			string[] accountEmails = this.Instance.ParentInstance.Configuration.Options["Adwords.Email"].Split('|');
			this.Delivery.Parameters["accountEmails"] = accountEmails;
			#endregion

			#region Nice to have params

			//Check for includeZeroImpression
			string includeZeroImpression;
			if (!String.IsNullOrEmpty(includeZeroImpression = Instance.ParentInstance.Configuration.Options["includeZeroImpression"]))
			{
				this.Delivery.Parameters["includeZeroImpression"] = includeZeroImpression;
			}
			this.Delivery.Parameters["includeZeroImpression"] = false;

			//Check for includeConversionTypes
			string includeConversionTypes;
			if (!String.IsNullOrEmpty(includeConversionTypes = Instance.ParentInstance.Configuration.Options["includeConversionTypes"]))
			{
				this.Delivery.Parameters["includeConversionTypes"] = includeConversionTypes;
			}
			this.Delivery.Parameters["includeConversionTypes"] = false; // deafult

			//Check for includeDisplaytData
			string includeDisplaytData;
			if (!String.IsNullOrEmpty(includeDisplaytData = Instance.ParentInstance.Configuration.Options["includeDisplaytData"]))
			{
				this.Delivery.Parameters["includeDisplaytData"] = includeDisplaytData;
			}
			this.Delivery.Parameters["includeDisplaytData"] = false; // deafult

			#endregion

			//Creating Delivery files Per Email 
			foreach (string email in accountEmails)
			{
				foreach (GA.ReportDefinitionReportType reportType in (List<GA.ReportDefinitionReportType>)this.Delivery.Parameters["reportTypes"])
				{
					DeliveryFile file = new DeliveryFile();
					file.Name = reportType.ToString();
					file.Parameters.Add("Email", email);
					this.Delivery.Files.Add(file);
				}
				if (Boolean.Parse(includeConversionTypes)) // if AD Performance With conversion type is required 
				{
					DeliveryFile file = new DeliveryFile();
					file.Name = "AD_PERFORMANCE_REPORT_(Conversion)";
					file.Parameters.Add("Email", email);
					this.Delivery.Files.Add(file);
				}
				if (Boolean.Parse(includeDisplaytData)) // if AD Performance With conversion type is required 
				{
					DeliveryFile file = new DeliveryFile();
					file.Name = "MANAGED_PLACEMENTS_PERFORMANCE_REPORT";
					file.Parameters.Add("Email", email);
					this.Delivery.Files.Add(file);
				}
			}
			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;
		}

	}
}
