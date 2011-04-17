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
            _job.name = "Adword Creative Report";
            _job.selectedColumns = _report.selectedColumns;

            // Validate report.
            try
            {
                ReportService.validateReportJobRequest(_job);

                // Schedule report.
                long jobId = ReportService.scheduleReportJob(job);

                // Wait for report to finish.
                ReportService.ReportJobStatus status = _job.getReportJobStatus(jobId);
                while (status != ReportJobStatus.Completed &&
                    status != ReportJobStatus.Failed)
                {
                    Console.WriteLine(
                        "Report job status is \"" + status + "\".");
                    Thread.Sleep(30000);
                    status = service.getReportJobStatus(jobId);
                }

                if (status == ReportJobStatus.Failed)
                {
                    Console.WriteLine("Report job generation failed.");
                    System.Environment.Exit(0);
                }
            }


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
