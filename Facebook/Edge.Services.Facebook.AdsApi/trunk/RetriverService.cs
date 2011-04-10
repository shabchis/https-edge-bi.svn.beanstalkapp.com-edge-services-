using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using MyFacebook=Facebook;
using System.Net;
using System.IO;
using Edge.Data.Pipeline.Readers;
using System.Globalization;
using System.Web;
using System.Dynamic;

namespace Edge.Services.Facebook.AdsApi
{
	class RetriverService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			string baseAddress = this.Instance.Configuration.Options["BaseServiceAdress"];// @"http://api.facebook.com/restserver.php";
			double progress = 0;
			double interval = (this.Delivery.Files.Count / 100) - 0.0001;
			foreach (DeliveryFile file in this.Delivery.Files)
			{
				
				try
				{
					DownloadFile(baseAddress, ref progress, interval, file);
					
				}
				catch (WebException ex)
				{
					Console.WriteLine(ex.Message);
				}
			}
			DeliveryFile deliveryFile= this.Delivery.Files["GetAdGroups.xml"];
					
						var adGroupReader = new XmlChunkReader
				(deliveryFile.SavedPath,
				Instance.Configuration.Options["Facebook.Ads.GetAdGroups.xpath"],// ./Ads_getAdGroupCreatives_response/ads_creative
				XmlChunkReaderOptions.AttributesAsValues | XmlChunkReaderOptions.ElementsAsValues
				);
						using (adGroupReader)
						{
							List<string> adGroupsIds = new List<string>();
							int counter=1;
							List<DeliveryFile> deliveryFiles=new List<DeliveryFile>();
							while(adGroupReader.Read())
							{
								adGroupsIds.Add(adGroupReader.Current["adgroup_id"]);
								if (adGroupsIds.Count == 999)								
									CreateCreativeDeliveryFile(ref adGroupsIds,ref counter,ref deliveryFiles);								
							}
							if (adGroupsIds.Count>0)
								CreateCreativeDeliveryFile(ref adGroupsIds,ref counter,ref deliveryFiles);
							if (deliveryFiles.Count > 0)
							{
								//TODO: PROGRESS TALK WITH DORON
								foreach (DeliveryFile file in deliveryFiles)
								{
									this.Delivery.Files.Add(file);
									DownloadFile(baseAddress,ref progress, interval, file);
									
								}
							}
						}
					

			return ServiceOutcome.Success;
		}

		private void DownloadFile(string baseAddress, ref double progress, double interval, DeliveryFile file)
		{
			HttpWebResponse response = null;
			string body = file.Parameters["body"].ToString();
			HttpWebRequest request = CreateRequest(baseAddress, body);
			response = (HttpWebResponse)request.GetResponse();
			FileManager.Download(response.GetResponseStream(), null);
			progress += interval;
			this.ReportProgress(progress);
		}

		private void CreateCreativeDeliveryFile(ref List<string> adGroupsIds,ref int counter,ref List<DeliveryFile> deliveryFiles)
		{
			DeliveryFile current = new DeliveryFile();
			current.Name = string.Format("GetAdGroupCreatives-{0}.xml", counter);
			current.Parameters.Add("body", GetAdGroupCreativesBody(adGroupsIds));
			deliveryFiles.Add(current);
			adGroupsIds.Clear();
			counter++;
		}

		private object GetAdGroupCreativesBody(List<string> adGroupsIds)
		{
			string body;
			Dictionary<string, string> AdGroupCreativesParameters = new Dictionary<string, string>();
			AdGroupCreativesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupCreativesParameters.Add("method", "facebook.ads.getAdGroupCreatives");
			AdGroupCreativesParameters.Add("include_deleted", "false");			
			dynamic d = new ExpandoObject();
			d.adgroup_ids = adGroupsIds;

			AdGroupCreativesParameters.Add("adgroup_ids", Newtonsoft.Json.JsonConvert.SerializeObject(d.adgroup_ids));
			body = CreateHTTPParameterList(AdGroupCreativesParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());

			return body;
		}
		private HttpWebRequest CreateRequest(string baseAddress,string body)
		{
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(baseAddress);
			request.Method = "POST";
			request.ContentType = "application/x-www-form-urlencoded";
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
