using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;
using System.Net;
using System.IO;
using Edge.Data.Pipeline;
using Newtonsoft.Json;

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
			
			BatchDownloadOperation countBatch = new BatchDownloadOperation();
			foreach (DeliveryFile file in Delivery.Files)
			{				
					FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters["URL"].ToString()+"limit=0"));
					countBatch.Add(fileDownloadOperation);				
			}
			countBatch.Progressed += new EventHandler(counted_Batch_Progressed);
			countBatch.Start();
			countBatch.Wait();
			countBatch.EnsureSuccess();
			List<DeliveryFile> files = new List<DeliveryFile>();
			foreach (DeliveryFile file in Delivery.Files)
			{
				using (StreamReader reader=new StreamReader(file.OpenContents()))
				{
					int offset = 0;
					int limit = 500;
					MyType t = JsonConvert.DeserializeObject<MyType>(reader.ReadToEnd());
					while (offset<t.count)
					{
						DeliveryFile f= new DeliveryFile();
						f.Name = string.Format(file.Name,offset);
						f.Parameters.Add("URL", string.Format("{0}&limit={1}&offset={2}", file.Parameters["URL"], limit, offset));
						files.Add(f);
						offset += limit;
					}
					offset = 0;				
				}
			}
			BatchDownloadOperation batch = new BatchDownloadOperation();
			foreach (var file in files)			
				this.Delivery.Files.Add(file);
			this.Delivery.Save();

			foreach (var file in files)
			{
				FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters["URL"].ToString()));
				batch.Add(fileDownloadOperation);
			}

			


			batch.Progressed += new EventHandler(batch_Progressed);
			batch.Start();
			batch.Wait();
			batch.EnsureSuccess();

			this.Delivery.Save();
			return ServiceOutcome.Success;
		}

		void batch_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation batchDownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(batchDownloadOperation.Progress * firstBatchRatio);
		}

		private HttpWebRequest CreateRequest(string baseUrl, string[] extraParams = null)
		{			
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(string.Format("{0}&{1}", baseUrl, _accessToken));
			return request;
		}
		void counted_Batch_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation batchDownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(batchDownloadOperation.Progress * firstBatchRatio);
		}



		
	}
	public class MyType
	{
		public string[] data;
		public int limit;
		public int offset;
		public int count;
		public class paging
		{
			public string next;
			public string previous;
		}

	}
}
