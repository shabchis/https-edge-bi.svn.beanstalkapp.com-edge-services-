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
			if (this.Delivery != null)
				this.Delivery.Delete();

			this.Delivery = new Delivery(Instance.InstanceID);
			this.Delivery.TargetLocationDirectory = "AdwordsSearch";
			this.Delivery.TargetPeriod = this.TargetPeriod;

#if (DEBUG)
			this.Delivery.Account = new Edge.Data.Objects.Account() { ID = 95};
#else
			this.Delivery.Account = new Edge.Data.Objects.Account() { ID = Instance.AccountID};
#endif

			//=============================== TEMP FOR DEBUG ===================================
			this.Delivery._guid = Guid.Parse(this.Instance.Configuration.Options["DeliveryGuid"]);
			//================================TEMP FOR DEBUG ===================================

			
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
			bool includeZeroImpression;
			try
			{
				if (!(includeZeroImpression = Boolean.Parse(Instance.ParentInstance.Configuration.Options["includeZeroImpression"])))
				{
					this.Delivery.Parameters["includeZeroImpression"] = includeZeroImpression;
				}
			}
			catch (ArgumentNullException)
			{
				//includeZeroImpression does not exists in configuration
				this.Delivery.Parameters["includeZeroImpression"] = false; // deafult
			}

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
			
			}

			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;

		}

	}
}
