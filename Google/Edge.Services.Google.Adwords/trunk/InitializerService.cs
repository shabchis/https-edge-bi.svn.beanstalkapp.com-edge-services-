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
		//AccountEntity _edgeAccount;
		//AdwordsReport googleReport;
		//List<ReportDefinitionReportType> _reportsTypes = new List<ReportDefinitionReportType>();
		//ReportDefinitionDateRangeType DateRange;
		//string AdwordsEmail;
		//long ReportId;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			if (this.Delivery != null)
				this.Delivery.Delete();

			this.Delivery = new Delivery(Instance.InstanceID);
			this.Delivery.TargetLocationDirectory = "AdwordsSearch";
			//googleReport = new AdwordsReport();
			IntializingParmeters();
			//_edgeAccount = new AccountEntity(Instance.AccountID, AdwordsEmail);

			this.Delivery.TargetPeriod = this.TargetPeriod;
#if (DEBUG)
			this.Delivery.Account = new Edge.Data.Objects.Account() { ID = 67};
#else
			this.Delivery.Account = new Edge.Data.Objects.Account() { ID = Instance.AccountID};
#endif

			this.Delivery.Files.Add(new DeliveryFile() { Name= "chicken shit" });


			//=============================== TEMP FOR DEBUG ===================================
			this.Delivery._guid = Guid.Parse(this.Instance.Configuration.Options["DeliveryGuid"]);
			//================================TEMP FOR DEBUG ===================================

			// TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO 
			/*
			// Get all report types from config
			string[] reportTypeNames = Instance.ParentInstance.Configuration.Options["Adwords.ReportType"].Split('|');
			List<GA.ReportDefinitionReportType> reportTypes = new List<GA.ReportDefinitionReportType>();
			foreach (string reportTypeName in reportTypeNames)
			{
				if (Enum.IsDefined(typeof(GA.ReportDefinitionReportType), reportTypeName))
					reportTypes.Add((GA.ReportDefinitionReportType)Enum.Parse(typeof(GA.ReportDefinitionReportType), reportTypeName, true));
				else throw new Exception("Undefined ReportType");
			}

			// Get date ranges in google format
			GA.ReportDefinitionDateRangeType gaDateRange = GA.ReportDefinitionDateRangeType.CUSTOM_DATE;
			string gaStartDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
			string gaEndDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");

			AccountEntity edgeAccount = new AccountEntity(Instance.AccountID, Instance.ParentInstance.Configuration.Options["Adwords.Email"]);

			foreach (string email in edgeAccount.Emails)
			{
				List<DeliveryFile> filesPerEmail = new List<DeliveryFile>();
				foreach (GA.ReportDefinitionReportType type in reportTypes)
				{
					_googleReport.SetReportDefinition(email, _dateRange, type);
					_reportId = _googleReport.intializingGoogleReport(Instance.AccountID, Instance.InstanceID);

					//FOR DEBUG
					//_googleReport.DownloadReport(_reportId);

					GoogleRequestEntity request = _googleReport.GetReportUrlParams(true);
					SetDeliveryFile(filesPerEmail, request);
					DeliveryFile file = new DeliveryFile();
					file.Name = _googleReport.Name;
					file.SourceUrl = request.downloadUrl.ToString();
					file.Parameters.Add("GoogleRequestEntity", request);

					filesPerEmail.Add(file);
					this.Delivery.Files.Add(file);

				}
				Delivery.Parameters.Add(email, filesPerEmail);
			}

			// Create all delivery files
			foreach (string email in _emails)
			{
				foreach (string reportType in _reportTypes)
				{
				}
			}
			*/
			// TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO // TODO 

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
