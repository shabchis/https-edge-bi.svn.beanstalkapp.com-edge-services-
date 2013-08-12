using System;
using System.Collections.Generic;
using System.Text;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;
using Edge.Data.Pipeline;

namespace Edge.Services.Facebook.GraphApi
{
	public class InitializerService : PipelineService
	{
		#region Data Members
		private Uri _baseAddress; 
		#endregion

		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			#region Init General
			if (!Configuration.TimePeriod.HasValue)
				throw new Exception("No time period is set for Service");

			if (!Configuration.Parameters.ContainsKey("AccountID"))
				throw new Exception("Missing Configuration Param: AccountID");

			if (!Configuration.Parameters.ContainsKey(FacebookConfigurationOptions.Account_ID))
				throw new Exception(String.Format("Missing Configuration Param: {0}", FacebookConfigurationOptions.Account_ID));

			if (!Configuration.Parameters.ContainsKey(Const.DeliveryServiceConfigurationOptions.FileDirectory))
				throw new Exception(String.Format("Missing Configuration Param: {0}", Const.DeliveryServiceConfigurationOptions.FileDirectory));

			if (!Configuration.Parameters.ContainsKey(FacebookConfigurationOptions.BaseServiceAddress))
				throw new Exception(String.Format("Missing Configuration Param: {0}", FacebookConfigurationOptions.BaseServiceAddress));

			var accountId = Configuration.Parameters.Get("AccountID", false, -1);
			var channelId = Configuration.Parameters.Get("ChannelID", false, -1);
			var facebookAccountId = Configuration.Parameters.Get<string>(FacebookConfigurationOptions.Account_ID);

			Delivery = NewDelivery();
			Delivery.Account = new Account { ID = accountId };
			//{
			//	ID = this.AccountID,
			//	OriginalID = this.Instance.Configuration.Options[FacebookConfigurationOptions.Account_ID].ToString()
			//};
			Delivery.Channel = new Channel { ID = channelId };
			Delivery.TimePeriodDefinition = Configuration.TimePeriod.Value;
			Delivery.FileDirectory = Configuration.Parameters.Get<string>(Const.DeliveryServiceConfigurationOptions.FileDirectory);

			Delivery.Outputs.Add(new DeliveryOutput
				{
					Signature = Delivery.CreateSignature(String.Format("facebook-[{0}]-[{1}]-[{2}]",
						accountId,
						facebookAccountId,
						Delivery.TimePeriodDefinition.ToAbsolute())),
					TimePeriodStart = Delivery.TimePeriodStart,
					TimePeriodEnd = Delivery.TimePeriodEnd,
					Account = Delivery.Account,
					Channel = Delivery.Channel

				});

			// Create an import manager that will handle rollback, if necessary
			HandleConflicts(new MetricsDeliveryManager(InstanceID), DeliveryConflictBehavior.Abort);

			_baseAddress = new Uri(Configuration.Parameters.Get<string>(FacebookConfigurationOptions.BaseServiceAddress));
			Progress = 0.2;
			#endregion

			#region Init Delivery Files

			var methodParams = new Dictionary<string, string>();
			var deliveryFile = new DeliveryFile();
			string methodUrl;

			#region AdGroupStats

			deliveryFile.Name = Consts.DeliveryFilesNames.AdGroupStats;
			methodParams.Add(Consts.FacebookMethodsParams.StartTime, ConvertToFacebookDateTime(Delivery.TimePeriodDefinition.Start.ToDateTime()));
			methodParams.Add(Consts.FacebookMethodsParams.EndTime, ConvertToFacebookDateTime(Delivery.TimePeriodDefinition.End.ToDateTime()));
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			methodParams.Add(Consts.FacebookMethodsParams.StatsMode, "with_delivery");
			methodUrl = string.Format("act_{0}/{1}", facebookAccountId, Consts.FacebookMethodsNames.GetAdGroupStats);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Enum.Parse(typeof(Consts.FileTypes), Consts.FileTypes.AdGroupStats.ToString()));
			Delivery.Files.Add(deliveryFile);

			Progress = 0.4;
			#endregion

			#region AdGroup

			deliveryFile = new DeliveryFile { Name = Consts.DeliveryFilesNames.AdGroup };
			methodUrl = string.Format("act_{0}/{1}", facebookAccountId, Consts.FacebookMethodsNames.GetAdGroups);
			if (Configuration.Parameters.ContainsKey(FacebookConfigurationOptions.AdGroupFields))
				methodParams.Add(Consts.FacebookMethodsParams.Fields, Configuration.Parameters.Get<string>(FacebookConfigurationOptions.AdGroupFields));
			
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Consts.FileTypes.AdGroups);
			Delivery.Files.Add(deliveryFile);

			Progress = 0.6;
			#endregion

			#region Campaigns
			deliveryFile = new DeliveryFile { Name = Consts.DeliveryFilesNames.Campaigns };
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			if (Configuration.Parameters.ContainsKey(FacebookConfigurationOptions.CampaignFields))
				methodParams.Add(Consts.FacebookMethodsParams.Fields, Configuration.Parameters.Get<string>(FacebookConfigurationOptions.CampaignFields));
			
			methodUrl = string.Format("act_{0}/{1}", facebookAccountId, Consts.FacebookMethodsNames.GetCampaigns);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Consts.FileTypes.Campaigns);
			Delivery.Files.Add(deliveryFile);

			Progress = 0.7;
			#endregion

			#region Creatives
			deliveryFile = new DeliveryFile { Name = Consts.DeliveryFilesNames.Creatives };
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			if (Configuration.Parameters.ContainsKey(FacebookConfigurationOptions.AdGroupCreativeFields))
				methodParams.Add(Consts.FacebookMethodsParams.Fields, Configuration.Parameters.Get<string>(FacebookConfigurationOptions.AdGroupCreativeFields));
		
			methodUrl = string.Format("act_{0}/{1}", facebookAccountId, Consts.FacebookMethodsNames.GetAdGroupCreatives);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Consts.FileTypes.Creatives);
			Delivery.Files.Add(deliveryFile);
			#endregion

			Progress = 0.9;
			#endregion

			Delivery.Save();
			return ServiceOutcome.Success;
		} 
		#endregion

		#region Private Methods
		private string GetMethodUrl(string relativeUrl, Dictionary<string, string> methodParams)
		{
			var urlParams = new StringBuilder();
			urlParams.Append(relativeUrl);
			urlParams.Append("?");
			foreach (var param in methodParams)
			{
				urlParams.Append(param.Key);
				urlParams.Append("=");
				urlParams.Append(param.Value);
				urlParams.Append("&");
			}
			var uri = new Uri(_baseAddress, urlParams.ToString());
			methodParams.Clear();
			return uri.ToString();

		}

		private string ConvertToFacebookDateTime(DateTime value)
		{
			if (!Configuration.Parameters.ContainsKey("TimeZone"))
				throw new Exception("Time zone must be configured! for utc please put 0!");
			var timeZone = Configuration.Parameters.Get<int>("TimeZone");

			if (!Configuration.Parameters.ContainsKey("Offset"))
				throw new Exception("Offset must be configured!for non offset please put 0");
			var offset = Configuration.Parameters.Get<int>("Offset");

			value = value.AddHours(-timeZone);
			value = value.AddHours(offset);
			return value.ToString("yyyy-MM-ddTHH:mm:ss");
		} 
		#endregion
	}
}
