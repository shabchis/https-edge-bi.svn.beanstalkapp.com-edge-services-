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
using System.Collections;

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

			_accessToken = this.Instance.Configuration.Options[FacebookConfigurationOptions.AccessToken];
			
			BatchDownloadOperation countBatch = new BatchDownloadOperation();

			var toremove = from f in Delivery.Files
						   where !f.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length)
						   select f.Name;
			foreach (string item in toremove.ToList())
			{
				Delivery.Files.Remove(item);
				
			}
			
			foreach (DeliveryFile file in Delivery.Files)
			{
				if (file.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length))
				{
					FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString() + "limit=0"));
					countBatch.Add(fileDownloadOperation);	 
				}			
			}
			countBatch.Progressed += new EventHandler(counted_Batch_Progressed);
			countBatch.Start();
			countBatch.Wait();
			countBatch.EnsureSuccess();
			List<DeliveryFile> files = new List<DeliveryFile>();
			Dictionary<Consts.FileTypes,List<string>> filesByType=new Dictionary<Consts.FileTypes,List<string>>();
			Delivery.Parameters.Add("FilesByType", filesByType);

			foreach (DeliveryFile file in Delivery.Files.Where(f => f.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length)))
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
						f.Parameters.Add(Consts.DeliveryFileParameters.Url, string.Format("{0}&limit={1}&offset={2}", file.Parameters[Consts.DeliveryFileParameters.Url], limit, offset));
						f.Parameters.Add(Consts.DeliveryFileParameters.FileSubType,(long) Consts.FileSubType.Data);
						f.Parameters.Add(Consts.DeliveryFileParameters.FileType,Enum.Parse(typeof(Consts.FileTypes), file.Parameters[Consts.DeliveryFileParameters.FileType].ToString()));
						files.Add(f);
						if (!filesByType.ContainsKey((Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType]))						
							filesByType.Add((Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType],new List<string>());
						filesByType[(Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType]].Add(f.Name);
						
						offset += limit;
					}
					offset = 0;				
				}
			}
			BatchDownloadOperation batch = new BatchDownloadOperation();
			foreach (var file in files)			
				this.Delivery.Files.Add(file);
			this.Delivery.Save();

			foreach (DeliveryFile file in files.Where(fi => fi.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Data)))
			{
				FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString()));
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
