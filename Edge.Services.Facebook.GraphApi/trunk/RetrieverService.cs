using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;
using System.Net;
using System.IO;
using Edge.Data.Pipeline;

namespace Edge.Services.Facebook.GraphApi
{
	class RetrieverService : PipelineService
	{
		private Uri _baseAddress;
		private string _accessToken;
		const double firstBatchRatio = 0.5;
		protected override ServiceOutcome DoPipelineWork()
		{
			_baseAddress = new Uri(this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress]);

			//Get Access token

			string urlAut = string.Format(string.Format(this.Delivery.Parameters[FacebookConfigurationOptions.Auth_AuthenticationUrl].ToString(),
				this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(),
				this.Delivery.Parameters[FacebookConfigurationOptions.Auth_RedirectUri],
				this.Delivery.Parameters[FacebookConfigurationOptions.Auth_AppSecret].ToString(),
				this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString()));

			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(urlAut);
			WebResponse response = request.GetResponse();

			using (StreamReader stream = new StreamReader(response.GetResponseStream()))
			{
				_accessToken = stream.ReadToEnd();
			}




			FileDownloadOperation adgroupDownload = null;
			BatchDownloadOperation batch = new BatchDownloadOperation();


			foreach (DeliveryFile file in Delivery.Files)
			{
				
					FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters["URL"].ToString()));
					batch.Add(fileDownloadOperation);
				
			}
			batch.Progressed += new EventHandler(batch_Progressed);

			batch.Start();
			adgroupDownload.Wait();
			adgroupDownload.EnsureSuccess();

			

			this.Delivery.Save();
			return ServiceOutcome.Success;
		}

		private HttpWebRequest CreateRequest(string baseUrl, string[] extraParams = null)
		{			
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(string.Format("{0}&{1}", baseUrl, _accessToken));
			return request;
		}
		void batch_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation batchDownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(batchDownloadOperation.Progress * firstBatchRatio);
		}



		
	}
}
