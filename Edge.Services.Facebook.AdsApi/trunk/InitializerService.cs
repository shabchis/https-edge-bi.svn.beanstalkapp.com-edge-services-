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
using Edge.Services.AdMetrics;


namespace Edge.Services.Facebook.AdsApi
{
	public class InitializerService: PipelineService	
	{
		protected override ServiceOutcome  DoPipelineWork()
		{
			// ...............................
			// SETUP
			this.Delivery = this.NewDelivery();
			
			// This is for finding conflicting services
			this.Delivery.Signature = String.Format("facebook-[{0}]-[{1}]-[{2}]",
				this.Instance.AccountID,
				this.Instance.Configuration.Options[FacebookConfigurationOptions.Account_ID].ToString(),
				this.TargetPeriod.ToAbsolute());

			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new AdMetricsImportManager.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			// ...............................

			// Now that we have a new delivery, start adding values
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				OriginalID = this.Instance.Configuration.Options[FacebookConfigurationOptions.Account_ID].ToString()
			};
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = 6
			};

			this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			// Copy some options as delivery parameters
			var configOptionsToCopyToDelivery = new string[] {
				FacebookConfigurationOptions.Account_ID,
				FacebookConfigurationOptions.Account_Name,
				FacebookConfigurationOptions.Auth_ApiKey,
				FacebookConfigurationOptions.Auth_AppSecret,
				FacebookConfigurationOptions.Auth_SessionKey,
				FacebookConfigurationOptions.Auth_SessionSecret,
			};
			foreach (string option in configOptionsToCopyToDelivery)
				this.Delivery.Parameters[option] = this.Instance.Configuration.Options[option];

			this.ReportProgress(0.2);
			DeliveryFile deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.AdGroupStats;
			deliveryFile.Parameters.Add("body", GetAdGroupStatsHttpRequest());
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.4);

			deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.AdGroup;
			deliveryFile.Parameters.Add("body", GetAdGroupsHttpRequest());

			this.Delivery.Files.Add(deliveryFile);


			this.ReportProgress(0.6);
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.Campaigns;
			deliveryFile.Parameters.Add("body", GetCampaignsHttpRequest());
			this.Delivery.Files.Add(deliveryFile);


			deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.AdGroupTargeting;
			deliveryFile.Parameters.Add("body", GetgetAdGroupTargeting());

			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.9);
			this.Delivery.Save();

			this.ReportProgress(1);

			return ServiceOutcome.Success;
		}
		

		

		private string GetAdGroupStatsHttpRequest()
		{
			
			string body;
			Dictionary<string, string> AdGroupStatesParameters = new Dictionary<string, string>();
			AdGroupStatesParameters.Add("account_id", this.Delivery.Account.OriginalID.ToString());
			AdGroupStatesParameters.Add("method",Consts.FacebookMethodsNames.GetAdGroupStats);
			AdGroupStatesParameters.Add("include_deleted", "true");
			AdGroupStatesParameters.Add("stats_mode", "with_delivery");
			dynamic timeRangeIn = new ExpandoObject();
			timeRangeIn.day_start = new { month =  TargetPeriod.Start.ToDateTime().Month, day =TargetPeriod.Start.ToDateTime().Day, year = TargetPeriod.Start.ToDateTime().Year };
			timeRangeIn.day_stop = new { month = TargetPeriod.End.ToDateTime().Month, day =  TargetPeriod.End.ToDateTime().Day, year = TargetPeriod.End.ToDateTime().Year };
			dynamic timeRange = new ExpandoObject();
			timeRange.time_range = timeRangeIn;
			string timeRangeString = Newtonsoft.Json.JsonConvert.SerializeObject(timeRange);
			AdGroupStatesParameters.Add("time_ranges", timeRangeString);
			body = CreateHTTPParameterList(AdGroupStatesParameters, this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString());
			

			return body;
		}
		private string GetAdGroupCreativesHttpRequest()
		{
			
			string body;
			Dictionary<string, string> AdGroupCreativesParameters = new Dictionary<string, string>();
			AdGroupCreativesParameters.Add("account_id", this.Delivery.Account.OriginalID.ToString());
			AdGroupCreativesParameters.Add("method", Consts.FacebookMethodsNames.GetAdGroupCreatives);
			AdGroupCreativesParameters.Add("include_deleted", "true");			
			body = CreateHTTPParameterList(AdGroupCreativesParameters, this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString());
			
			return body;
		}
		private string GetAdGroupsHttpRequest()
		{
			
			string body;
			Dictionary<string, string> AdGroupsParameters = new Dictionary<string, string>();
			AdGroupsParameters.Add("account_id", this.Delivery.Account.OriginalID.ToString());
			AdGroupsParameters.Add("method", Consts.FacebookMethodsNames.GetAdGroups);
			AdGroupsParameters.Add("include_deleted", "true");
			body = CreateHTTPParameterList(AdGroupsParameters, this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString());
			
			return body;
		}
		private string GetCampaignsHttpRequest()
		{
			
			string body;
			Dictionary<string, string> CampaignsParmaters = new Dictionary<string, string>();
			CampaignsParmaters.Add("account_id", this.Delivery.Account.OriginalID.ToString());
			CampaignsParmaters.Add("method", Consts.FacebookMethodsNames.GetCampaigns);
			CampaignsParmaters.Add("include_deleted", "true");


			body = CreateHTTPParameterList(CampaignsParmaters, this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString());
			
			return body;


		}
		private string GetgetAdGroupTargeting()
		{
			string body;
			Dictionary<string, string> AdGroupTargetingParameters = new Dictionary<string, string>();
			AdGroupTargetingParameters.Add("account_id", this.Delivery.Account.OriginalID.ToString());
			AdGroupTargetingParameters.Add("method", Consts.FacebookMethodsNames.GetAdGroupTargeting);
			AdGroupTargetingParameters.Add("include_deleted", "true");
			body = CreateHTTPParameterList(AdGroupTargetingParameters, this.Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionKey].ToString(), this.Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret].ToString());

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
