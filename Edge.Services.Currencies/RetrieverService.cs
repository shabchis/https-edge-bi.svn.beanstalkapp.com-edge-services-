using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Net;
using Edge.Data.Pipeline;

namespace Edge.Services.Currencies
{
    class RetrieverService : PipelineService
    {

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {

            //XigniteCurrenciesService.XigniteCurrenciesSoap currenciesService;
            //Header header = new Header();
            //header.Username = this.Delivery.Parameters["UserName"].ToString();
            //header.Password = this.Delivery.Parameters["UserPassword"].ToString();

            //string symbols = this.Delivery.Parameters["CrossRateSymbols"].ToString();

            //GetAverageHistoricalCrossRatesRequest request =
            //    new GetAverageHistoricalCrossRatesRequest(header, symbols, StartDate: "07/21/2012", EndDate: "07/21/2012");


            //var a = currenciesService.GetAverageHistoricalCrossRates(request);


            // Create a batch and use its progress as the service's progress
            BatchDownloadOperation batch = new BatchDownloadOperation();
            batch.Progressed += new EventHandler((sender, e) =>
            {
                this.ReportProgress(batch.Progress * 0.95);
            });

            foreach (DeliveryFile file in this.Delivery.Files)
            {
                WebRequest request = WebRequest.Create(file.SourceUrl);
                request.ContentType = file.Parameters["Content-Type"].ToString();
                request.Method = "POST";

                byte[] bytes = Encoding.UTF8.GetBytes(file.Parameters["Body"].ToString());
                request.ContentLength = bytes.Length;

                using (var stream = request.GetRequestStream())
                {
                    stream.Write(bytes, 0, bytes.Length);
                }

                //Headers
                request.Headers.Add("SOAPAction", file.Parameters["SOAPAction"].ToString());

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
