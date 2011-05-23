using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Google.Api.Ads.AdWords.v201101;

namespace Edge.Services.Google.Adwords
{
	public class InitializerService : PipelineService
	{
		AccountEntity EdgeAccount;
		AdwordsReport googleReport;
		List<ReportDefinitionReportType> ReportsTypes = new List<ReportDefinitionReportType>();
		ReportDefinitionDateRangeType DateRange;
		string AdwordsEmail;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			IntializingParmeters();
			this.Delivery = new Delivery(Instance.InstanceID);
			EdgeAccount = new AccountEntity(Instance.AccountID);

			foreach (string email in EdgeAccount.Emails)
			{
				foreach (ReportDefinitionReportType type in ReportsTypes)
				{
					googleReport = new AdwordsReport(email, DateRange, type);
					googleReport.CreateGoogleReport(Instance.AccountID, Instance.InstanceID);
					this.Delivery.TargetPeriod = this.TargetPeriod;
					this.Delivery.Parameters.Add(type.ToString(), googleReport.Id);
				}
			}


			//googleReport.DownloadReport(51015891);



			this.Delivery.Parameters["AccountID"] = Instance.AccountID;

			//this.Delivery.Files.Add(new DeliveryFile()
			//{
			//    Name = googleReport.Name,
			//    SourceUrl = url // TODO: get from API
			//});

			// TEMP FOR DEBUG
			this.Delivery._guid = Guid.Parse(this.Instance.Configuration.Options["DeliveryGuid"]);

			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;

		}

		private void IntializingParmeters()
		{
			//Geting Report Types
			string Reports;
			if (!String.IsNullOrEmpty(Reports = Instance.ParentInstance.Configuration.Options["Adwords.ReportType"]))
			{
				foreach (string type in Reports.Split('|').ToList<string>())
				{
					if (Enum.IsDefined(typeof(ReportDefinitionReportType), type))
						ReportsTypes.Add((ReportDefinitionReportType)Enum.Parse(typeof(ReportDefinitionReportType), type, true));
					else throw new Exception("Undefined ReportType");
				}
			}

			//Getting Date Range
			if (Enum.IsDefined(typeof(ReportDefinitionReportType), this.Instance.ParentInstance.Configuration.Options["Adwords.DateRange"]))
				DateRange = (ReportDefinitionDateRangeType)Enum.Parse(typeof(ReportDefinitionDateRangeType), this.Instance.ParentInstance.Configuration.Options["Adwords.DateRange"], true);
			else throw new Exception("Undefined DateRange ");
			
			if (DateRange.Equals(ReportDefinitionDateRangeType.CUSTOM_DATE))
			{
				string TargetPeriod = this.Instance.ParentInstance.Configuration.Options["TargetPeriod"];
				if (!String.IsNullOrEmpty(TargetPeriod))
				{
					//TO DO : Cast from TargetPeriod to Google start & end date
				}
				else throw new Exception("Undefined TargetPeriod");
			}

			//Getting Emails
			if (!String.IsNullOrEmpty(this.AdwordsEmail = Instance.Configuration.Options["Adwords.Email"]))
				EdgeAccount.Emails.Add(AdwordsEmail);
			else throw new Exception("Undefined AdwordsEmail ");

		}
	}
}
