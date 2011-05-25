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
		long ReportId;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			this.Delivery = new Delivery(Instance.InstanceID);
			EdgeAccount = new AccountEntity(Instance.AccountID);
			IntializingParmeters();

			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Parameters["AccountID"] = Instance.AccountID;

			//=============================== TEMP FOR DEBUG ===================================
			this.Delivery._guid = Guid.Parse(this.Instance.Configuration.Options["DeliveryGuid"]);
			//================================TEMP FOR DEBUG ===================================

			foreach (string email in EdgeAccount.Emails)
			{
				DeliveryFileList FilesPerEmail = new DeliveryFileList();
				foreach (ReportDefinitionReportType type in ReportsTypes)
				{
					googleReport = new AdwordsReport(email, DateRange, type);
					ReportId = googleReport.intializingGoogleReport(Instance.AccountID, Instance.InstanceID);
					GoogleRequestEntity request = googleReport.GetReportUrlParams(true);

					DeliveryFile file = new DeliveryFile();
					file.Name = googleReport.Name;
					file.SourceUrl = request.downloadUrl.ToString();
					file.Parameters.Add("GoogleRequestEntity", request);
					file.Parameters.Add("Path", "D:\\");

					FilesPerEmail.Add(file);
					this.Delivery.Files.Add(file);

				}
				Delivery.Parameters.Add(email, FilesPerEmail);
			}


			//googleReport.DownloadReport(51015891);

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

			//intialzing Date Range
			DateRange = ReportDefinitionDateRangeType.CUSTOM_DATE;
			try
			{
				this.googleReport.StartDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
				this.googleReport.EndDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");
			}
			catch (Exception e)
			{
				throw new Exception("Cannot set start/end time from TargetPeriod", e);
			}


			//Getting Date Range - ONLY FOR DEBUG.
			if (Enum.IsDefined(typeof(ReportDefinitionDateRangeType), Instance.ParentInstance.Configuration.Options["Adwords.DateRange"]))
				DateRange = (ReportDefinitionDateRangeType)Enum.Parse(typeof(ReportDefinitionDateRangeType), this.Instance.ParentInstance.Configuration.Options["Adwords.DateRange"], true);
			else throw new Exception("Undefined DateRange ");
			//Getting Date Range - ONLY FOR DEBUG.

			//Getting Emails
			if (!String.IsNullOrEmpty(this.AdwordsEmail = Instance.ParentInstance.Configuration.Options["Adwords.Email"]))
				EdgeAccount.Emails.Add(AdwordsEmail);
			else throw new Exception("Undefined AdwordsEmail ");

		}
	}
}
