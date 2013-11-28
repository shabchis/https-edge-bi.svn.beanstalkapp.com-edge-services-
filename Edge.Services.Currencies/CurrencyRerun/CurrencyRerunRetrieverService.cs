﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Net;
using Edge.Data.Pipeline;

namespace Edge.Services.Currencies
{
    class CurrencyRerunRetrieverService : PipelineService
    {

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {

            // Create a batch and use its progress as the service's progress
            BatchDownloadOperation batch = new BatchDownloadOperation();
            batch.Progressed += new EventHandler((sender, e) =>
            {
                this.ReportProgress(batch.Progress * 0.95);
            });

            foreach (DeliveryFile file in this.Delivery.Files)
            {
                WebRequest request = WebRequest.Create(file.SourceUrl);
                this.Delivery.Save();
                DeliveryFileDownloadOperation download = file.Download(request);
                //download.Ended += new EventHandler(download_Ended);
                batch.Add(download);
            }

            batch.Start();
            batch.Wait();

            this.Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }
    }
}
