using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Deliveries;

namespace Edge.Services.Google.Adwords
{
    public class InitializerService : PipelineService
    {
        AccountEntity account;
		AdwordsReport googleReport;

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            account = new AccountEntity(Instance.AccountID);

			//TO DO : Get Report type from configuration
			//Instance.Configuration.Options["Adwords.ReportType"];
			googleReport = new AdwordsReport(account.Emails);

            this.Delivery.Parameters["AccountID"] = account.Id;

            //Instance.Configuration.Options["Adwords.Email"]
            //Initializing Delivery
            this.Delivery = new Delivery(Instance.InstanceID);
            this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Parameters.Add("AD_PERFORMANCE_REPORT_ID", googleReport.Id);
		
			//this.Delivery.Files.Add(new DeliveryFile()
			//{
			//    Name = googleReport.Name,
			//    SourceUrl = url // TODO: get from API
			//});

            this.Delivery.Save();

            return Core.Services.ServiceOutcome.Success;

        }
    }
}
