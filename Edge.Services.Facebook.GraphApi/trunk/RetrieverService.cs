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
using FileInfo = Edge.Data.Pipeline.FileInfo;


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
        static int counter = 1000;
	   // private List<DeliveryFile> files;
        Dictionary<Consts.FileTypes, List<string>> filesByType = new Dictionary<Consts.FileTypes, List<string>>();
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


            //files = new List<DeliveryFile>();
            Delivery.Parameters.Add("FilesByType", filesByType);
		    FileDownloadOperation fileDownloadOperation;

			foreach (DeliveryFile file in Delivery.Files)
			{
				if (file.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length))
				{
                    fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString()));
					countBatch.Add(fileDownloadOperation);
				}

                var fileTypeStr =  Convert.ToString(file.Parameters[Consts.DeliveryFileParameters.FileType]);

                Consts.FileTypes fileType = (Consts.FileTypes)Enum.Parse(typeof(Consts.FileTypes), fileTypeStr);

                if (!filesByType.ContainsKey(fileType))
                    filesByType.Add(fileType,new List<string>());

                filesByType[fileType].Add(file.Name);              
			}
			countBatch.Progressed += new EventHandler(counted_Batch_Progressed);
            RunBatch(countBatch);


		    var nextfiles = Delivery.Files.Where(f => f.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long) Consts.FileSubType.Length));

            Next(nextfiles);

            this.Delivery.Save();

		    
            

            ////batch.Progressed += new EventHandler(batch_Progressed);
            //RunBatch(batch);
            //this.Delivery.Save();
			return ServiceOutcome.Success;
		}
        private void Next(IEnumerable<DeliveryFile> nextfiles)
        {
            if (nextfiles.Count() == 0)
                return;

            var newFiles = new List<DeliveryFile>();

            foreach (DeliveryFile file in nextfiles)
            {                
                counter = counter + 1;
                CreateNextFiles(file, counter, newFiles);
            }

            if (newFiles.Count() == 0)
            {
                return;
            }

            foreach (var file in newFiles)
                this.Delivery.Files.Add(file);
            this.Delivery.Save();

            BatchDownloadOperation batch = new BatchDownloadOperation();
            foreach (DeliveryFile file in newFiles.Where(fi => fi.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.New)))
            {
                file.Parameters[Consts.DeliveryFileParameters.FileSubType] = Consts.FileSubType.Data;
                FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString()));
                batch.Add(fileDownloadOperation);
            }

            RunBatch(batch);

            Next(newFiles);

        }

        private void CreateNextFiles(DeliveryFile file, int counter,List<DeliveryFile> list)
        {
            string next = GetNextUrl(file);

            if (!string.IsNullOrEmpty(next))
            {
                var url = FixUrlChars(next);
                var name = file.Name.Substring(0, file.Name.IndexOf('-'));
                var nameFormat = "{0}-{1}.json";
                var fileName = string.Format(nameFormat, name,counter);
                
                var fileType = file.Parameters[Consts.DeliveryFileParameters.FileType].ToString();
                var f = CreateDeliveryFile(fileName, url, fileType);


                if (!filesByType.ContainsKey((Consts.FileTypes) f.Parameters[Consts.DeliveryFileParameters.FileType]))
                    filesByType.Add((Consts.FileTypes) f.Parameters[Consts.DeliveryFileParameters.FileType],
                                    new List<string>());
                filesByType[(Consts.FileTypes) f.Parameters[Consts.DeliveryFileParameters.FileType]].Add(f.Name);

                list.Add(f);
            }
        }

       

	    #endregion
        private string GetNextUrl(DeliveryFile file)
        {
            string next = string.Empty;

            using (StreamReader reader = new StreamReader(file.OpenContents()))
            {
                string json = reader.ReadToEnd();
                dynamic dynamicObject = JsonConvert.DeserializeObject(json);

                if(dynamicObject.paging != null)
                    next = dynamicObject.paging.next;
            }

            return next;
        }
        private DeliveryFile CreateDeliveryFile(string name, string url, string type)
        {
             var f = new DeliveryFile();
            f.Name = name;
            f.Parameters.Add(Consts.DeliveryFileParameters.Url, string.Format("{0}", url));
            f.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, (long)Consts.FileSubType.New);
            f.Parameters.Add(Consts.DeliveryFileParameters.FileType, Enum.Parse(typeof(Consts.FileTypes), type));
            return f;
        }

        private void RunBatch(BatchDownloadOperation batch)
        {
            batch.Start();
            batch.Wait();
            batch.EnsureSuccess();
        }

        private FileInfo AddDataFilesToBatch(BatchDownloadOperation batch, DeliveryFile file)
        {
            if (file.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Data))
            {
                FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString()));
                batch.Add(fileDownloadOperation);
                return fileDownloadOperation.FileInfo;
            }

            return null;

        }


       

        private string FixUrlChars(string url)
        {
            url = url.Replace("\u0025", "%");
            return url.Replace("\\", "");
        }

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
