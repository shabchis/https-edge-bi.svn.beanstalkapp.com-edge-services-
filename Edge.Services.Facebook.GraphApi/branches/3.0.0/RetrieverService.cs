using System;
using System.Collections.Generic;
using System.Linq;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;
using System.Net;
using System.IO;
using Edge.Data.Pipeline;
using Newtonsoft.Json;

namespace Edge.Services.Facebook.GraphApi
{
	public class RetrieverService : PipelineService
	{
		#region Data Members
		private string _accessToken;
		const double FIRST_BATCH_RATIO = 0.5;
		const int ROW_LIMIT = 500; 
		#endregion

		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			//Get Access token
			if (!Configuration.Parameters.ContainsKey(FacebookConfigurationOptions.AccessToken))
				throw new Exception(String.Format("Missing Configuration Param: {0}", FacebookConfigurationOptions.AccessToken));
			_accessToken = Configuration.Parameters.Get<string>(FacebookConfigurationOptions.AccessToken);

			#region Get file legth
			// download legth files to get no. of rows in each file (FB limit up to 500 records per file)
			// (regular file which contains rows counter at hte end of the file)
			var countBatch = new BatchDownloadOperation();
			var toRemove = from f in Delivery.Files
						   where !f.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length)
						   select f.Name;
			foreach (var item in toRemove.ToList())
			{
				Delivery.Files.Remove(item);
			}

			foreach (var file in Delivery.Files)
			{
				if (file.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length))
				{
					FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url] + "limit=1"));
					countBatch.Add(fileDownloadOperation);
				}
			}
			countBatch.Progressed += batch_Progressed;
			countBatch.Start();
			countBatch.Wait();
			countBatch.EnsureSuccess();
			#endregion

			// download data
			var files = new List<DeliveryFile>();
			var filesByType = new Dictionary<Consts.FileTypes, List<string>>();
			Delivery.Parameters.Add("FilesByType", filesByType);

			foreach (var file in Delivery.Files.Where(f => f.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Length)))
			{
				using (var reader = new StreamReader(file.OpenContents()))
				{
					var offset = 0;
					var json = reader.ReadToEnd();
					var t = JsonConvert.DeserializeObject<FileMetadata>(json);
					while (offset < t.Count)
					{
						var f = new DeliveryFile { Name = string.Format(file.Name, offset) };
						f.Parameters.Add(Consts.DeliveryFileParameters.Url, string.Format("{0}&limit={1}&offset={2}", file.Parameters[Consts.DeliveryFileParameters.Url], ROW_LIMIT, offset));
						f.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, (long)Consts.FileSubType.Data);
						f.Parameters.Add(Consts.DeliveryFileParameters.FileType, Enum.Parse(typeof(Consts.FileTypes), file.Parameters[Consts.DeliveryFileParameters.FileType].ToString()));
						files.Add(f);
						if (!filesByType.ContainsKey((Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType]))
							filesByType.Add((Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType], new List<string>());
						filesByType[(Consts.FileTypes)f.Parameters[Consts.DeliveryFileParameters.FileType]].Add(f.Name);

						offset += ROW_LIMIT;
					}
				}
			}
			var batch = new BatchDownloadOperation();
			foreach (var file in files)
				Delivery.Files.Add(file);
			Delivery.Save();

			foreach (var file in files.Where(fi => fi.Parameters[Consts.DeliveryFileParameters.FileSubType].Equals((long)Consts.FileSubType.Data)))
			{
				FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest(file.Parameters[Consts.DeliveryFileParameters.Url].ToString()));
				batch.Add(fileDownloadOperation);
			}

			batch.Progressed += batch_Progressed;
			batch.Start();
			batch.Wait();
			batch.EnsureSuccess();

			Delivery.Save();
			return ServiceOutcome.Success;
		} 
		#endregion

		#region Private Methods/Events
		private void batch_Progressed(object sender, EventArgs e)
		{
			var batchDownloadOperation = (BatchDownloadOperation)sender;
			Progress = batchDownloadOperation.Progress * FIRST_BATCH_RATIO;
		}

		private HttpWebRequest CreateRequest(string baseUrl)
		{
			var request = (HttpWebRequest)WebRequest.Create(string.Format("{0}&access_token={1}", baseUrl, _accessToken));
			return request;
		} 
		#endregion
	}

	public class FileMetadata
	{
		public int Limit;
		public int Offset;
		public int Count;
	}
}
