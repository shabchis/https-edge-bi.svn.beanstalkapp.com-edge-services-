using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using System.Web;
using System.Globalization;
using System.Net;
using System.IO;

namespace Edge.Services.Facebook.AdsApi
{
	public class InitializerService: PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			//TODO: TALK WITH DORON I THINK ITS IS PARENTINSTANCEID
			// Create a new delivery
			this.Delivery = new Delivery(this.Instance.InstanceID)
			{
				TargetPeriod = this.TargetPeriod
			};

			//set parameters for entire delivery
			this.Delivery.Parameters["AccountID"] = this.Instance.AccountID;

			if (this.Instance.Configuration.Options["APIKey"] == null)
				this.Delivery.Parameters["APIKey"] = this.Instance.ParentInstance.Configuration.Options["APIKey"].ToString();
			else
				this.Delivery.Parameters["APIKey"] = this.Instance.Configuration.Options["APIKey"].ToString();


			if (this.Instance.Configuration.Options["sessionKey"] == null)
				this.Delivery.Parameters["sessionKey"] = this.Instance.ParentInstance.Configuration.Options["sessionKey"].ToString();
			else
				this.Delivery.Parameters["sessionKey"] = Instance.Configuration.Options["sessionKey"].ToString();

			if (this.Instance.Configuration.Options["applicationSecret"] == null)
				this.Delivery.Parameters["applicationSecret"] = this.Instance.ParentInstance.Configuration.Options["applicationSecret"].ToString();
			else
				this.Delivery.Parameters["applicationSecret"] = this.Instance.Configuration.Options["applicationSecret"].ToString();

			if (Instance.Configuration.Options["FBaccountID"] == null)
				this.Delivery.Parameters["FBaccountID"] = this.Instance.ParentInstance.Configuration.Options["FBaccountID"].ToString();
			else
				this.Delivery.Parameters["FBaccountID"] = this.Instance.Configuration.Options["FBaccountID"].ToString();


			if (Instance.Configuration.Options["accountName"] == null)
				this.Delivery.Parameters["accountName"] = this.Instance.ParentInstance.Configuration.Options["accountName"].ToString();
			else
				this.Delivery.Parameters["accountName"] = this.Instance.Configuration.Options["accountName"].ToString();

			if (Instance.Configuration.Options["sessionSecret"] == null)
				this.Delivery.Parameters["sessionSecret"] = this.Instance.ParentInstance.Configuration.Options["sessionSecret"].ToString();
			else
				this.Delivery.Parameters["sessionSecret"] = this.Instance.Configuration.Options["sessionSecret"].ToString();

			this.Delivery.ChannelID =int.Parse(this.Instance.Configuration.Options["ChannelID"]);

			string baseAddress = this.Instance.Configuration.Options["BaseServiceAdress"];// @"http://api.facebook.com/restserver.php";
			this.ReportProgress(0.2);


			DeliveryFile deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroupStats";
			deliveryFile.Parameters.Add("AdGroupStatsHttpRequest", GetAdGroupStatsHttpRequest(baseAddress));
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.4);
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroupCreatives";
			deliveryFile.Parameters.Add("AdGroupCreativesHttpRequest", GetAdGroupCreativesHttpRequest(baseAddress));
			this.Delivery.Files.Add(deliveryFile);
			this.ReportProgress(0.6);

			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroups";
			deliveryFile.Parameters.Add("AdGroupsHttpRequest", GetAdGroupsHttpRequest(baseAddress));
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.8);
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetCampaigns";
			deliveryFile.Parameters.Add("CampaignsHttpRequest", GetCampaignsHttpRequest(baseAddress));
			this.Delivery.Files.Add(deliveryFile);
			this.ReportProgress(0.98);
			this.Delivery.Save();
			this.ReportProgress(0.99);
			return ServiceOutcome.Success;
		}

		private HttpWebRequest GetAdGroupStatsHttpRequest(string baseAddress)
		{
			HttpWebRequest request = CreateRequest(baseAddress);
			string body;
			Dictionary<string, string> AdGroupStatesParameters = new Dictionary<string, string>();
			AdGroupStatesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupStatesParameters.Add("method", "facebook.ads.getAdGroupStats");			
			AdGroupStatesParameters.Add("campaign_ids", "");
			AdGroupStatesParameters.Add("adgroup_ids", "");
			AdGroupStatesParameters.Add("include_deleted","false");			
			string timeRange = string.Format("\"time_ranges\": { \"day_start\":{ :{\"month\":{0},\"day\":{1},\"year\":{2}},\"day_stop\":{\"month\":{3},\"day\":{4},\"year\":{5}}}}", TargetPeriod.Start.ToDateTime().Month, TargetPeriod.Start.ToDateTime().Day, TargetPeriod.End.ToDateTime().Year, TargetPeriod.End.ToDateTime().Month, TargetPeriod.End.ToDateTime().Day, TargetPeriod.End.ToDateTime().Year);
			AdGroupStatesParameters.Add("time_ranges", timeRange);
			body = CreateHTTPParameterList(AdGroupStatesParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			CreateBody(request.GetRequestStream(),body);

			return request;
		}


		private HttpWebRequest GetAdGroupCreativesHttpRequest(string baseAddress)
		{
			HttpWebRequest request = CreateRequest(baseAddress);
			string body;
			Dictionary<string, string> AdGroupCreativesParameters = new Dictionary<string, string>();
			AdGroupCreativesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupCreativesParameters.Add("method", "facebook.ads.getAdGroupCreatives");
			AdGroupCreativesParameters.Add("include_deleted", "false");
			//TODO:  for API bug 2011-03-21 TALK WITH DORON MAX CAMPAIGNS PER ARRAY
			AdGroupCreativesParameters.Add("campaign_ids", "");		
			AdGroupCreativesParameters.Add("adgroup_ids", "");
			body = CreateHTTPParameterList(AdGroupCreativesParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			CreateBody(request.GetRequestStream(), body);
			return request;
		}
		private HttpWebRequest GetAdGroupsHttpRequest(string baseAddress)
		{
			HttpWebRequest request = CreateRequest(baseAddress);
			string body;
			Dictionary<string, string> AdGroupsParameters = new Dictionary<string, string>();
			AdGroupsParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupsParameters.Add("method", "facebook.ads.getAdGroups");
			AdGroupsParameters.Add("include_deleted", "false");
			AdGroupsParameters.Add("campaign_ids", "");
			AdGroupsParameters.Add("adgroup_ids", "");

			body = CreateHTTPParameterList(AdGroupsParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			CreateBody(request.GetRequestStream(), body);
			return request;
		}
		private HttpWebRequest GetCampaignsHttpRequest(string baseAddress)
		{
			HttpWebRequest request = CreateRequest(baseAddress);
			string body;
			Dictionary<string, string> CampaignsParmaters = new Dictionary<string, string>();
			CampaignsParmaters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			CampaignsParmaters.Add("method", "facebook.ads.getCampaigns");
			CampaignsParmaters.Add("include_deleted", "false");
			CampaignsParmaters.Add("campaign_ids", "");

			body = CreateHTTPParameterList(CampaignsParmaters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
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

		private HttpWebRequest CreateRequest(string baseAddress)
		{
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(baseAddress);
			request.Method = "POST";
			request.ContentType = "application/x-www-form-urlencoded";

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
