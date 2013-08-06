using System;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using Edge.Core.Services;

namespace Edge.Services.BackOffice.EasyForex
{
	public class RetrieverService : PipelineService
	{
		#region Override Methids
		protected override ServiceOutcome DoPipelineWork()
		{

			// Create a batch and use its progress as the service's progress
			var batch = new BatchDownloadOperation();
			batch.Progressed += (sender, e) =>
				{
					Progress = batch.Progress * 0.95;
				};

			foreach (var file in Delivery.Files)
			{
				if (String.IsNullOrWhiteSpace(file.SourceUrl))
					continue;

				var request = WebRequest.Create(file.SourceUrl);
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

				// TODO: shirat - to remove? why saving delivery between files? 
				Delivery.Save();
				var download = file.Download(request);
				download.Ended += download_Ended;
				batch.Add(download);
			}

			batch.Start();
			batch.Wait();

			Delivery.Save();

			return ServiceOutcome.Success;
		} 
		#endregion

		private void download_Ended(object sender, EventArgs e)
		{
			// TODO: shirat - what is the purpose of this event?
			// Add a retrieved history entry to every file
			//((DeliveryFileDownloadOperation)sender).DeliveryFile.History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
		}
	}
}
