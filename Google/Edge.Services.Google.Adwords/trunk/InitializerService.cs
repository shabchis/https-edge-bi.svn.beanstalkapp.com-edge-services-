using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using GA = Google.Api.Ads.AdWords.v201101;

namespace Edge.Services.Google.Adwords
{
	public class InitializerService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			this.Delivery = new Delivery(Instance.InstanceID, this.DeliveryID);
			this.Delivery.TargetLocationDirectory = "AdwordsSearch";
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Account = new Edge.Data.Objects.Account() { ID = Instance.AccountID };

			//Get MCC Email
			if (String.IsNullOrEmpty(Instance.ParentInstance.Configuration.Options["Adwords.MccEmail"]))
				throw new Exception("Missing Configuration Param , Adwords.MccEmail");
			else this.Delivery.Parameters["MccEmail"] = Instance.ParentInstance.Configuration.Options["Adwords.MccEmail"];

			// Get all report types from config
			string[] reportTypeNames = Instance.ParentInstance.Configuration.Options["Adwords.ReportType"].Split('|');
			List<GA.ReportDefinitionReportType> reportTypes = new List<GA.ReportDefinitionReportType>();
			foreach (string reportTypeName in reportTypeNames)
			{
				if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
					reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
				else throw new Exception("Undefined ReportType");
			}
			this.Delivery.Parameters["reportTypes"] = reportTypes;

			//Get all Account Emails
			string[] accountEmails = Instance.ParentInstance.Configuration.Options["Adwords.Email"].Split('|');
			this.Delivery.Parameters["accountEmails"] = accountEmails;

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


			//Creating Delivery files Per Email 
			foreach (string email in accountEmails)
			{
				foreach (GA.ReportDefinitionReportType reportType in reportTypes)
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
