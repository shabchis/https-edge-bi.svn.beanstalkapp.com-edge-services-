﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using GA = Google.Api.Ads.AdWords.v201101;

namespace Edge.Services.Google.Adwords
{
	public class GoogleSearchDeliveryManager : DeliveryManager
	{

		public override void ApplyUniqueness(Delivery delivery)
		{
			delivery.TargetLocationDirectory = "AdwordsSearch";
			delivery.TargetPeriod = CurrentService.TargetPeriod;
			delivery.Account = new Edge.Data.Objects.Account() { ID = CurrentService.Instance.AccountID };
			delivery.Channel = new Data.Objects.Channel() { ID = 1 };

			#region Must Have Params

			//Get MCC Email
			if (String.IsNullOrEmpty(CurrentService.Instance.ParentInstance.Configuration.Options["Adwords.MccEmail"]))
				throw new Exception("Missing Configuration Param , Adwords.MccEmail");
			else delivery.Parameters["MccEmail"] = CurrentService.Instance.ParentInstance.Configuration.Options["Adwords.MccEmail"];

			// Get Report types
			string[] reportTypeNames = CurrentService.Instance.ParentInstance.Configuration.Options["Adwords.ReportType"].Split('|');
			List<GA.ReportDefinitionReportType> reportTypes = new List<GA.ReportDefinitionReportType>();
			foreach (string reportTypeName in reportTypeNames)
			{
				if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
					reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
				else throw new Exception("Undefined ReportType");
			}
			delivery.Parameters["reportTypes"] = reportTypes;

			//Get Account Emails
			string[] accountEmails = CurrentService.Instance.ParentInstance.Configuration.Options["Adwords.Email"].Split('|');
			delivery.Parameters["accountEmails"] = accountEmails;
			#endregion
		}
	}
	public class InitializerService : BaseInitializerService
	{

		public override DeliveryManager GetDeliveryManager()
		{
			return new GoogleSearchDeliveryManager();
		}

		public override void ApplyDeliveryDetails()
		{
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
			foreach (string email in (string[])this.Delivery.Parameters["accountEmails"])
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
			//return Core.Services.ServiceOutcome.Success;
		}
	}
}
