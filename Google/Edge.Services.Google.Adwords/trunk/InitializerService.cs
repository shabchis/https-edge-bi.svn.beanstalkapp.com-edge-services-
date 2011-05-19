using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;

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

            

            //Instance.Configuration.Options["Adwords.Email"]
            //Initializing Delivery
            this.Delivery = new Delivery(Instance.InstanceID);
            this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Parameters.Add("AD_PERFORMANCE_REPORT_ID", googleReport.Id);

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
    }
}
