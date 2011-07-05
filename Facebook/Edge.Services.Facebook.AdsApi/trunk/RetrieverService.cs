﻿using System;
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
		private string _baseAddress;
		const double firstBatchRatio = 0.5;
		protected override ServiceOutcome DoPipelineWork()
		{
			// http://api.facebook.com/restserver.php
			_baseAddress = this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress];


			
			FileDownloadOperation adgroupDownload=null; //= new FileDownloadOperation(CreateRequest("adgroups blah blah"));
			BatchDownloadOperation batch = new BatchDownloadOperation();
			//adgroupDownload.Ended += new EventHandler<EndedEventArgs>(adgroupDownload_Ended);

			foreach (DeliveryFile file in Delivery.Files)
			{
				if (file.Name == Consts.DeliveryFilesNames.AdGroup)
				{
					adgroupDownload = file.Download(CreateRequest());
					adgroupDownload.RequestBody = file.Parameters[Consts.DeliveryFileParameters.Body].ToString();
					batch.Insert(0, adgroupDownload);
				}
				else
				{
					FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest());
					fileDownloadOperation.RequestBody = file.Parameters[Consts.DeliveryFileParameters.Body].ToString();
					batch.Add(fileDownloadOperation);
				}
			}
			batch.Progressed += new EventHandler(batch_Progressed);
			
			batch.Start();
			adgroupDownload.Wait();
			adgroupDownload.EnsureSuccess();
			
			BatchDownloadOperation creativeBatch = new BatchDownloadOperation();
			creativeBatch.Progressed += new EventHandler(creativeBatch_Progressed);
			DeliveryFile adGroupFile = Delivery.Files[Consts.DeliveryFilesNames.AdGroup];
			
			var adGroupReader = new XmlDynamicReader
				(FileManager.Open(adGroupFile.Location),
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
						// CreateCreativeDeliveryFile also adds the new delivery file to this.Delivery.Files
						deliveryFiles.Add(CreateCreativeDeliveryFile(ref adGroupsIds, ref counter));
				}

				if (adGroupsIds.Count > 0)
					deliveryFiles.Add(CreateCreativeDeliveryFile(ref adGroupsIds, ref counter));

				if (deliveryFiles.Count > 0)
				{
					this.Delivery.Save();
					foreach (DeliveryFile file in deliveryFiles)
					{
						FileDownloadOperation fileDownloadOperation = file.Download(CreateRequest());
						fileDownloadOperation.RequestBody = file.Parameters[Consts.DeliveryFileParameters.Body].ToString();
						creativeBatch.Add(fileDownloadOperation);
					}
				}		
			}

			creativeBatch.Start();

			batch.Wait();
			creativeBatch.Wait();

			batch.EnsureSuccess();
			creativeBatch.EnsureSuccess();

			this.Delivery.Save();
			return ServiceOutcome.Success;
		}

		void creativeBatch_Progressed(object sender, EventArgs e)
		{
			FileDownloadOperation fileDownloadOperation = (FileDownloadOperation)sender;
			this.ReportProgress(fileDownloadOperation.Progress * (1 - firstBatchRatio) + firstBatchRatio);
		}

		void batch_Progressed(object sender, EventArgs e)
		{
			FileDownloadOperation fileDownloadOperation = (FileDownloadOperation)sender;
			this.ReportProgress(fileDownloadOperation.Progress * firstBatchRatio);
		}		

		

		private DeliveryFile CreateCreativeDeliveryFile(ref List<string> adGroupsIds, ref int counter)
		{
			DeliveryFile current = new DeliveryFile();
			current.Name = string.Format(Consts.DeliveryFilesNames.Creatives, counter++);
			current.Parameters.Add(Consts.DeliveryFileParameters.Body, GetAdGroupCreativesBody(adGroupsIds));
			current.Parameters.Add("IsCreativeDeliveryFile", true);
			adGroupsIds.Clear();
			if (Delivery.Files.Contains(current.Name))
				Delivery.Files.Remove(current.Name);
			
			this.Delivery.Files.Add(current);
			return current;
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
		private HttpWebRequest CreateRequest()
		{
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(_baseAddress);
			request.Method = "POST";
			request.ContentType = "application/x-www-form-urlencoded";
			//request.Accept = "text/xml;charset=utf-8";
			
			return request;
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
