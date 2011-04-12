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
using System.Dynamic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Deliveries;

namespace Edge.Services.Facebook.AdsApi
{
	public class InitializerService: PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
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

			//this.Delivery.Channel =(Edge.Data.Pipeline.Objects.Channel)this.Instance.Configuration.Options["ChannelID"];

			
			this.ReportProgress(0.2);


			DeliveryFile deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroupStats";
			deliveryFile.Parameters.Add("body", GetAdGroupStatsHttpRequest());
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.4);
			/* MOVED TO RETRIVER BECAUSE FACEBOOK BUG (NOT MORE THE 1000 CREATIVES)
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroupCreatives.xml";
			deliveryFile.Parameters.Add("body", GetAdGroupCreativesHttpRequest());
			this.Delivery.Files.Add(deliveryFile);*/

			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroups";
			deliveryFile.Parameters.Add("body", GetAdGroupsHttpRequest());
			this.Delivery.Files.Add(deliveryFile);
			

			this.ReportProgress(0.6);
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetCampaigns";
			deliveryFile.Parameters.Add("body", GetCampaignsHttpRequest());
			this.Delivery.Files.Add(deliveryFile);


			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "getAdGroupTargeting";
			deliveryFile.Parameters.Add("body", GetgetAdGroupTargeting());
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.98);
			this.Delivery.Save();
			this.ReportProgress(0.99);
			return ServiceOutcome.Success;
		}

		private string GetAdGroupStatsHttpRequest()
		{
			
			string body;
			Dictionary<string, string> AdGroupStatesParameters = new Dictionary<string, string>();
			AdGroupStatesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupStatesParameters.Add("method", "facebook.ads.getAdGroupStats");				
			AdGroupStatesParameters.Add("include_deleted","false");
			dynamic timeRange = new ExpandoObject();
			timeRange.day_start = new { month =  TargetPeriod.Start.ToDateTime().Month, day =TargetPeriod.Start.ToDateTime().Day, year = TargetPeriod.Start.ToDateTime().Year };
			timeRange.day_stop = new { month = TargetPeriod.End.ToDateTime().Month, day =  TargetPeriod.End.ToDateTime().Day, year = TargetPeriod.End.ToDateTime().Year };
			AdGroupStatesParameters.Add("time_ranges", Newtonsoft.Json.JsonConvert.SerializeObject(timeRange));
			body = CreateHTTPParameterList(AdGroupStatesParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			

			return body;
		}
		private string GetAdGroupCreativesHttpRequest()
		{
			
			string body;
			Dictionary<string, string> AdGroupCreativesParameters = new Dictionary<string, string>();
			AdGroupCreativesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupCreativesParameters.Add("method", "facebook.ads.getAdGroupCreatives");
			AdGroupCreativesParameters.Add("include_deleted", "false");			
			body = CreateHTTPParameterList(AdGroupCreativesParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			
			return body;
		}
		private string GetAdGroupsHttpRequest()
		{
			
			string body;
			Dictionary<string, string> AdGroupsParameters = new Dictionary<string, string>();
			AdGroupsParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupsParameters.Add("method", "facebook.ads.getAdGroups");
			AdGroupsParameters.Add("include_deleted", "false");
			body = CreateHTTPParameterList(AdGroupsParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			
			return body;
		}
		private string GetCampaignsHttpRequest()
		{
			
			string body;
			Dictionary<string, string> CampaignsParmaters = new Dictionary<string, string>();
			CampaignsParmaters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			CampaignsParmaters.Add("method", "facebook.ads.getCampaigns");
			CampaignsParmaters.Add("include_deleted", "false");
			

			body = CreateHTTPParameterList(CampaignsParmaters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());
			
			return body;


		}
		private string GetgetAdGroupTargeting()
		{
			string body;
			Dictionary<string, string> AdGroupTargetingParameters = new Dictionary<string, string>();
			AdGroupTargetingParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupTargetingParameters.Add("method", "facebook.ads.getAdGroupTargeting");
			AdGroupTargetingParameters.Add("include_deleted", "false");
			body = CreateHTTPParameterList(AdGroupTargetingParameters, this.Delivery.Parameters["APIKey"].ToString(), this.Delivery.Parameters["sessionKey"].ToString(), this.Delivery.Parameters["sessionSecret"].ToString());

			return body;
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
