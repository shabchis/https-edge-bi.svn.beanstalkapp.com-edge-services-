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
using System.Security.Cryptography;

namespace Edge.Services.Facebook.GraphApi
{
	class RetrieverService : PipelineService
	{
		#region Data Members
		private Uri _baseAddress;
		private string _accessToken;
		private string _appSecretProof;
		const double _firstBatchRatio = 0.5;
		const double _secondBatchRatio = 0.5; 
		#endregion

		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			_baseAddress = new Uri(this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress]);

			//Get Access token

			_accessToken = this.Instance.Configuration.Options[FacebookConfigurationOptions.Auth_AccessToken];
			_appSecretProof = GetAppSecretProof();

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
					FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString() + "limit=1"));
					countBatch.Add(fileDownloadOperation);
				}
			}
			countBatch.Progressed += new EventHandler(counted_Batch_Progressed);
			countBatch.Start();
			countBatch.Wait();
			countBatch.EnsureSuccess();
			List<DeliveryFile> files = new List<DeliveryFile>();
			Dictionary<Consts.FileTypes, List<string>> filesByType = new Dictionary<Consts.FileTypes, List<string>>();
			Delivery.Parameters.Add("FilesByType", filesByType);

			foreach (DeliveryFile file in Delivery.Files.Where(f => f.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length)))
			{
				using (StreamReader reader = new StreamReader(file.OpenContents()))
				{
					int offset = 0;
					int limit = 500;
					string json = reader.ReadToEnd();
					MyType t = JsonConvert.DeserializeObject<MyType>(json);
					while (offset < t.count)
					{
						DeliveryFile f = new DeliveryFile();
						f.Name = string.Format(file.Name, offset);
						f.Parameters.Add(Consts.DeliveryFileParameters.Url, string.Format("{0}&limit={1}&offset={2}", file.Parameters[Consts.DeliveryFileParameters.Url], limit, offset));
						f.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, (long)Consts.FileSubType.Data);
						f.Parameters.Add(Consts.DeliveryFileParameters.FileType, Enum.Parse(typeof(Consts.FileTypes), file.Parameters[Consts.DeliveryFileParameters.FileType].ToString()));
						files.Add(f);
						if (!filesByType.ContainsKey((Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType]))
							filesByType.Add((Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType], new List<string>());
						filesByType[(Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType]].Add(f.Name);

						offset += limit;
					}
					offset = 0;
				}
			}
			foreach (var file in files)
				this.Delivery.Files.Add(file);
			this.Delivery.Save();

			BatchDownloadOperation batch = new BatchDownloadOperation();
			foreach (DeliveryFile file in files.Where(fi => fi.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Data)))
			{
				FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString()));
				batch.Add(fileDownloadOperation);
			}

			//batch.Progressed += new EventHandler(batch_Progressed);
			batch.Start();
			batch.Wait();
			batch.EnsureSuccess();
			this.Delivery.Save();

			return ServiceOutcome.Success;
		} 
		#endregion

		#region Event Handlers
		void counted_Batch_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation batchDownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(batchDownloadOperation.Progress * _firstBatchRatio);
		} 

		void batch_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation batchDownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(batchDownloadOperation.Progress * _secondBatchRatio);
		}
		#endregion

		#region Private Methods
		private HttpWebRequest CreateRequest(string baseUrl, string[] extraParams = null)
		{
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(string.Format("{0}&access_token={1}", baseUrl, _accessToken));
			//HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(string.Format("{0}&access_token={1}&appsecret_proof={2}", baseUrl, _accessToken, _appSecretProof));
			return request;
		}

		private string GetAppSecretProof()
		{
            string appSecret = Instance.Configuration.Options[FacebookConfigurationOptions.Auth_AppSecret];
            var secretByte = Encoding.UTF8.GetBytes(appSecret);
            var hmacsha256 = new HMACSHA256(secretByte);
            var tokenBytes = Encoding.UTF8.GetBytes(_accessToken);
            hmacsha256.ComputeHash(tokenBytes);
            return ByteToString(hmacsha256.Hash);

          
		}

		private static string ByteToString(byte[] buff)
		{
			string sbinary = "";
			for (int i = 0; i < buff.Length; i++)
				sbinary += buff[i].ToString("X2"); /* hex format */
			return sbinary;
		}  
		#endregion
	}

	public class MyType
	{
		//public string[] data;
		//public class paging
		//{
		//	public string next;
		//	public string previous;
		//}
		public int limit;
		public int offset;
		public int count;
	}
}
