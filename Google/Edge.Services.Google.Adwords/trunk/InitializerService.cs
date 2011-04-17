using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Deliveries;

namespace Edge.Services.Google.Adwords
{
    public class InitializerService: PipelineService
    {
        AccountEntity _account;
        AdwordsReport _report = new AdwordsReport();

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            _account = new AccountEntity(Instance.AccountID);

            this.Delivery.Parameters["AccountID"] = _account.Id;

            //Instance.Configuration.Options["Adwords.Email"]
                       
            //Creating Report
            ReportService.DefinedReportJob _job = new ReportService.DefinedReportJob();
            _job.adWordsType = _report.AdWordsType;
            _job.selectedReportType = _report.ReportType.ToString();

            if (_account.Emails.Count > 1)
            {
                _job.crossClient = true; // if not supplied - clientEmails is being ignored.
                _job.clientEmails = _account.Emails.ToArray<string>();
            }

            _job.startDay = _report.StartDate;
            _job.endDay = _report.EndDate;

            //Initializing Delivery
            this.Delivery = new Delivery(Instance.InstanceID);
            this.Delivery.TargetPeriod = this.TargetPeriod;
            this.Delivery.Files.Add(new DeliveryFile()
            {
                Name = "AdwordsCreativeReport",
                SourceUrl = url // TODO: get from API
            });

            this.Delivery.Save();

            return Core.Services.ServiceOutcome.Success;

        }
    }
}
