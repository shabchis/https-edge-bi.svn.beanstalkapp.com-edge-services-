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
		//AccountEntity _edgeAccount;
		//AdwordsReport googleReport;
		//List<ReportDefinitionReportType> _reportsTypes = new List<ReportDefinitionReportType>();
		//ReportDefinitionDateRangeType DateRange;
		//string AdwordsEmail;
		//long ReportId;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			this.Delivery = new Delivery(Instance.InstanceID);
			this.Delivery.TargetLocationDirectory = "AdwordsSearch";
			//googleReport = new AdwordsReport();
			IntializingParmeters();
			//_edgeAccount = new AccountEntity(Instance.AccountID, AdwordsEmail);

			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Parameters["AccountID"] = Instance.ParentInstance.AccountID;


			//=============================== TEMP FOR DEBUG ===================================
			this.Delivery._guid = Guid.Parse(this.Instance.Configuration.Options["DeliveryGuid"]);
			//================================TEMP FOR DEBUG ===================================

			//foreach (string email in _edgeAccount.Emails)
			//{
			//    List<DeliveryFile> filesPerEmail = new List<DeliveryFile>();
			//    foreach (ReportDefinitionReportType type in _reportsTypes)
			//    {
			//        googleReport.SetReportDefinition(email, DateRange, type);
			//        ReportId = googleReport.intializingGoogleReport(Instance.AccountID, Instance.InstanceID);
			//        GoogleRequestEntity request = googleReport.GetReportUrlParams(true);

			//        DeliveryFile file = new DeliveryFile();
			//        file.Name = googleReport.Name;
			//        file.SourceUrl = request.downloadUrl.ToString();
			//        file.Parameters.Add("GoogleRequestEntity", request);

			//        // TEMP
			//        // TODO: file.Location = "Google/AdWords";
			//        file.Parameters.Add("Path", "Google");

			//        filesPerEmail.Add(file);
			//        this.Delivery.Files.Add(file);

			//    }
			//    Delivery.Parameters.Add(email, filesPerEmail);
			//}


			//googleReport.DownloadReport(51015891);

			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;

		}

		private void IntializingParmeters()
		{
			//Geting Report Types
			string reports;
			if (!String.IsNullOrEmpty(reports = Instance.ParentInstance.Configuration.Options["Adwords.ReportType"]))
				this.Delivery.Parameters["ReportsType"] = reports;
			else throw new Exception("Undefined ReportType");


			//Getting Date Range - ONLY FOR DEBUG.
			//if (Enum.IsDefined(typeof(ReportDefinitionDateRangeType), Instance.ParentInstance.Configuration.Options["Adwords.DateRange"]))
			//    DateRange = (ReportDefinitionDateRangeType)Enum.Parse(typeof(ReportDefinitionDateRangeType), this.Instance.ParentInstance.Configuration.Options["Adwords.DateRange"], true);
			//else throw new Exception("Undefined DateRange ");
			//Getting Date Range - ONLY FOR DEBUG.

			//Getting Emails
			string adwordsEmail;
			if (!String.IsNullOrEmpty(adwordsEmail = Instance.ParentInstance.Configuration.Options["Adwords.Email"]))
				this.Delivery.Parameters["AdwordsEmail"] = adwordsEmail;
			else throw new Exception("Undefined AdwordsEmail ");


		}
	}
}
