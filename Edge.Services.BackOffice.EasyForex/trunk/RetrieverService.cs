using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Net;
using Edge.Core.Services;
using System.Xml;

namespace Edge.Services.BackOffice.EasyForex
{
	class RetrieverService : PipelineService
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
				if (String.IsNullOrWhiteSpace(file.SourceUrl))
					continue;

				WebRequest request = WebRequest.Create(file.SourceUrl);
				request.ContentType = file.Parameters["Content-Type"].ToString();
                request.Method = "POST";

				byte[] bytes = Encoding.UTF8.GetBytes(file.Parameters["Body"].ToString());
				request.ContentLength = bytes.Length;

				using (var stream = request.GetRequestStream())
				{
					stream.Write(bytes,0,bytes.Length);
				}

				//Headers
				request.Headers.Add("SOAPAction", file.Parameters["SOAPAction"].ToString());

                this.Delivery.Save();
				DeliveryFileDownloadOperation download = file.Download(request);
				download.Ended += new EventHandler(download_Ended);
				batch.Add(download);
			}

			batch.Start();
			batch.Wait();

			// Add a retrieved history entry for the entire delivery
			
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

		void download_Ended(object sender, EventArgs e)
		{
			// Add a retrieved history entry to every file
			//((DeliveryFileDownloadOperation)sender).DeliveryFile.History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
		}
	}
}
