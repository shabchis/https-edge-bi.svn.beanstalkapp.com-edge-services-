using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using MyFacebook = Facebook;
using System.Net;
using System.IO;
using System.Globalization;
using System.Web;
using System.Dynamic;
using Edge.Data.Pipeline.Services;
using System.Threading;


namespace Edge.Services.Facebook.AdsApi
{
	class RetrieverService : PipelineService
	{
		private List<FileDownloadOperation> _operations = new List<FileDownloadOperation>();
		private int _operationsInProgress;
		private string _baseAddress;
		private AutoResetEvent _waitHandle;
        private bool _creativeDownloaded=false;

		protected override ServiceOutcome DoPipelineWork()
		{
			_operationsInProgress = this.Delivery.Files.Count;
			_baseAddress = this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress];// @"http://api.facebook.com/restserver.php";

			_waitHandle = new AutoResetEvent(false);
			foreach (DeliveryFile file in this.Delivery.Files)
			{

				try
				{
					if (file.Name != Consts.DeliveryFilesNames.adGroup)
					{
						DownloadFile(file);
						file.History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
					}
				}
				catch (WebException ex)
				{
					Edge.Core.Utilities.Log.Write(this.ToString(), string.Format("Error downloading file {0}", file.Name), ex, Core.Utilities.LogMessageType.Error);
				}
			}
			DownloadFile(Delivery.Files[Consts.DeliveryFilesNames.adGroup]);
			Delivery.Files[Consts.DeliveryFilesNames.adGroup].History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
			DeliveryFile deliveryFile = this.Delivery.Files[Consts.DeliveryFilesNames.adGroup];

			var adGroupReader = new XmlDynamicReader
				(FileManager.Open(deliveryFile.Location),
				Instance.Configuration.Options[FacebookConfigurationOptions.Ads_XPath_GetAdGroups]);// ./Ads_getAdGroupCreatives_response/ads_creative


			using (adGroupReader)
			{
				List<string> adGroupsIds = new List<string>();
				int counter = 1;
				List<DeliveryFile> deliveryFiles = new List<DeliveryFile>();
				while (adGroupReader.Read())
				{
					adGroupsIds.Add(adGroupReader.Current.adgroup_id);
					if (adGroupsIds.Count == 999)
						CreateCreativeDeliveryFile(ref adGroupsIds, ref counter, ref deliveryFiles);
				}
				if (adGroupsIds.Count > 0)
					CreateCreativeDeliveryFile(ref adGroupsIds, ref counter, ref deliveryFiles);
				if (deliveryFiles.Count > 0)
				{
					this.Delivery.Save();
					foreach (DeliveryFile file in deliveryFiles)
					{

						DownloadFile(file);
						file.History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
					}
				}
                _creativeDownloaded = true;
			}
			_waitHandle.WaitOne();

			this.Delivery.Save();
			return ServiceOutcome.Success;
		}



		private void DownloadFile(DeliveryFile file)
		{

			bool async = true;
			HttpWebResponse response = null;
			string body = file.Parameters["body"].ToString();
			HttpWebRequest request = CreateRequest(_baseAddress, body);
			try
			{
				response = (HttpWebResponse)request.GetResponse();
			}
			catch (WebException ex)
			{

				response = (HttpWebResponse)request.GetResponse();
			}

			if (file.Name == Consts.DeliveryFilesNames.adGroup)
				async = false;
			FileDownloadOperation fileDownloadOperation = file.Download(response.GetResponseStream(), async, response.ContentLength);
			_operations.Add(fileDownloadOperation);
			fileDownloadOperation.Progressed += new EventHandler<ProgressEventArgs>(fileDownloadOperation_Progressed);
			fileDownloadOperation.Ended += new EventHandler<EndedEventArgs>(fileDownloadOperation_Ended);
			fileDownloadOperation.Start();
			
		}

		void fileDownloadOperation_Ended(object sender, EndedEventArgs e)
		{
			_operationsInProgress -= 1;
			if (_operationsInProgress == 0 && _creativeDownloaded)
				_waitHandle.Set();

		}

		void fileDownloadOperation_Progressed(object sender, ProgressEventArgs e)
		{
			long downloaded = 0;
			long total = 0;

			_operations.All(operation =>
			{
				downloaded += operation.DownloadedBytes;
				total += operation.FileInfo.TotalBytes;
				return false;
			});

			double percent = downloaded / total;
			this.ReportProgress(percent);
		}

		private void CreateCreativeDeliveryFile(ref List<string> adGroupsIds, ref int counter, ref List<DeliveryFile> deliveryFiles)
		{
			DeliveryFile current = new DeliveryFile();
			current.Name = string.Format("AdGroupCreatives-{0}.xml", counter);
			current.Parameters.Add("body", GetAdGroupCreativesBody(adGroupsIds));
			current.Parameters.Add("IsCreativeDeliveryFile", true);
			
			deliveryFiles.Add(current);
			adGroupsIds.Clear();
            if (Delivery.Files.Contains(current.Name))
                Delivery.Files.Remove(current.Name);
            else
                _operationsInProgress += 1;
			this.Delivery.Files.Add(current);
			counter++;
		}

		private object GetAdGroupCreativesBody(List<string> adGroupsIds)
		{
			string body;
			Dictionary<string, string> AdGroupCreativesParameters = new Dictionary<string, string>();
			AdGroupCreativesParameters.Add("account_id", this.Delivery.Parameters[FacebookConfigurationOptions.Account_ID].ToString());
			AdGroupCreativesParameters.Add("method", Consts.FacebookMethodsNames.GetAdGroupCreatives);
			AdGroupCreativesParameters.Add("include_deleted", "false");
			dynamic d = new ExpandoObject();
			d.adgroup_ids = adGroupsIds;

			AdGroupCreativesParameters.Add("adgroup_ids", Newtonsoft.Json.JsonConvert.SerializeObject(d.adgroup_ids));
			body = CreateHTTPParameterList(AdGroupCreativesParameters, this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString());

			return body;
		}
		private HttpWebRequest CreateRequest(string baseAddress, string body)
		{
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(baseAddress);
			request.Method = "POST";
			request.ContentType = "application/x-www-form-urlencoded";
			//request.Accept = "text/xml;charset=utf-8";
			CreateBody(request.GetRequestStream(), body);
			return request;
		}
		private void CreateBody(System.IO.Stream stream, string body)
		{
			using (StreamWriter writer = new StreamWriter(stream))
			{
				writer.Write(body);
			}

		}
		internal string CreateHTTPParameterList(IDictionary<string, string> parameterList, string applicationKey, string sessionKey, string sessionSecret)
		{
			StringBuilder builder = new StringBuilder();
			parameterList.Add("api_key", applicationKey); //this.Session.ApplicationKey
			parameterList.Add("session_key", sessionKey); //this.Session.ApplicationKey
			parameterList.Add("v", "1.0");
			parameterList.Add("call_id", DateTime.Now.Ticks.ToString("x", CultureInfo.InvariantCulture));
			//if (this.Session.SessionSecret != null)
			if (sessionSecret != null)
			{
				parameterList.Add("ss", "1");
			}
			parameterList.Add("sig", this.GenerateSignature(parameterList, applicationKey, sessionSecret));
			foreach (KeyValuePair<string, string> pair in parameterList)
			{
				builder.Append(pair.Key);
				builder.Append("=");
				builder.Append(HttpUtility.UrlEncode(pair.Value));
				builder.Append("&");
			}
			builder.Remove(builder.Length - 1, 1);
			return builder.ToString();
		}

		internal string GenerateSignature(IDictionary<string, string> parameters, string applicationKey, string sessionSecret)
		{
			StringBuilder builder = new StringBuilder();
			List<string> list = ParameterDictionaryToList(parameters);
			list.Sort();
			foreach (string str in list)
			{
				builder.Append(string.Format(CultureInfo.InvariantCulture, "{0}={1}", new object[] { str, parameters[str] }));
			}
			builder.Append(sessionSecret);
			byte[] hash = MD5Core.GetHash(builder.ToString().Trim());
			builder = new StringBuilder();
			foreach (byte num in hash)
			{
				builder.Append(num.ToString("x2", CultureInfo.InvariantCulture));
			}
			return builder.ToString();
		}

		internal static List<string> ParameterDictionaryToList(IEnumerable<KeyValuePair<string, string>> parameterDictionary)
		{
			List<string> list = new List<string>();
			foreach (KeyValuePair<string, string> pair in parameterDictionary)
			{
				list.Add(string.Format(CultureInfo.InvariantCulture, "{0}", new object[] { pair.Key }));
			}
			return list;
		}
	}
}
