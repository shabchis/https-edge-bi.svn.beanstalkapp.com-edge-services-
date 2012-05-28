﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Dynamic;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Metrics;
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Common.Importing;

namespace Edge.Services.Facebook.GraphApi
{
	public class InitializerService : PipelineService
	{
		private Uri _baseAddress;
		protected override ServiceOutcome DoPipelineWork()
		{
			#region Init General
			// ...............................
			// SETUP
			this.Delivery = NewDelivery();
			// This is for finding conflicting services
			this.Delivery.Outputs.Add(new DeliveryOutput()
			{
				Signature = Delivery.CreateSignature(String.Format("facebook-[{0}]-[{1}]-[{2}]",
					this.Instance.AccountID,
					this.Instance.Configuration.Options[FacebookConfigurationOptions.Account_ID].ToString(),
					this.Delivery.TimePeriodDefinition.ToAbsolute())),
					TimePeriodStart=Delivery.TimePeriodStart,
					TimePeriodEnd=Delivery.TimePeriodEnd
					
			});

			// ...............................

			// Now that we have a new delivery, start adding values
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				OriginalID = this.Instance.Configuration.Options[FacebookConfigurationOptions.Account_ID].ToString()
			};
			this.Delivery.TimePeriodDefinition = this.TimePeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = 6
			};

			

			this.Delivery.FileDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[Edge.Data.Pipeline.Metrics.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			if (string.IsNullOrEmpty(this.Delivery.FileDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");
			// Copy some options as delivery parameters
			var configOptionsToCopyToDelivery = new string[] {
				FacebookConfigurationOptions.Account_ID,
				FacebookConfigurationOptions.Account_Name,
				FacebookConfigurationOptions.Auth_ApiKey,
				FacebookConfigurationOptions.Auth_AppSecret,
				FacebookConfigurationOptions.Auth_SessionKey,
				FacebookConfigurationOptions.Auth_SessionSecret,
				FacebookConfigurationOptions.Auth_RedirectUri,
				FacebookConfigurationOptions.Auth_AuthenticationUrl
			};
			foreach (string option in configOptionsToCopyToDelivery)
				this.Delivery.Parameters[option] = this.Instance.Configuration.Options[option];
			if (string.IsNullOrEmpty(this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress]))
				throw new Exception("facebook base url must be configured!");
			_baseAddress = new Uri(this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress]);
			this.ReportProgress(0.2);
			#endregion

			#region Init Delivery Files

			Dictionary<string, string> methodParams = new Dictionary<string, string>();
			string methodUrl;
			DeliveryFile deliveryFile = new DeliveryFile();
			#region adgroupstats

			deliveryFile.Name = Consts.DeliveryFilesNames.AdGroupStats;
			methodParams.Add(Consts.FacebookMethodsParams.StartTime, ConvertToFacebookDateTime(TimePeriod.Start.ToDateTime()));
			methodParams.Add(Consts.FacebookMethodsParams.EndTime, ConvertToFacebookDateTime(TimePeriod.End.ToDateTime()));
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			methodParams.Add(Consts.FacebookMethodsParams.StatsMode, "with_delivery");
			methodUrl = string.Format("act_{0}/{1}", Delivery.Account.OriginalID, Consts.FacebookMethodsNames.GetAdGroupStats);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Enum.Parse(typeof(Consts.FileTypes), Consts.FileTypes.AdGroupStats.ToString()));
			this.Delivery.Files.Add(deliveryFile);
			#endregion

			this.ReportProgress(0.4);
			#region adgroup

			deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.AdGroup;
			methodUrl = string.Format("act_{0}/{1}", Delivery.Account.OriginalID, Consts.FacebookMethodsNames.GetAdGroups);
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Consts.FileTypes.AdGroups);

			this.Delivery.Files.Add(deliveryFile);
			#endregion

			this.ReportProgress(0.6);

			#region Campaigns


			deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.Campaigns;
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			methodUrl = string.Format("act_{0}/{1}", Delivery.Account.OriginalID, Consts.FacebookMethodsNames.GetCampaigns);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Consts.FileTypes.Campaigns);
			this.Delivery.Files.Add(deliveryFile);

			#endregion

			#region Creatives
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = Consts.DeliveryFilesNames.Creatives;
			methodParams.Add(Consts.FacebookMethodsParams.IncludeDeleted, "true");
			methodUrl = string.Format("act_{0}/{1}", Delivery.Account.OriginalID, Consts.FacebookMethodsNames.GetAdGroupCreatives);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Length);
			deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileType, Consts.FileTypes.Creatives);
			this.Delivery.Files.Add(deliveryFile);
			#endregion

			//#region AdGroupTargeting
			//deliveryFile = new DeliveryFile();
			//deliveryFile.Name = Consts.DeliveryFilesNames.AdGroupTargeting;			
			//methodUrl = string.Format("act_{0}/{1}", Delivery.Account.OriginalID, Consts.FacebookMethodsNames.GetAdGroupTargeting);
			//deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.Url, GetMethodUrl(methodUrl, methodParams));
			//deliveryFile.Parameters.Add(Consts.DeliveryFileParameters.FileSubType, Consts.FileSubType.Data);
			//this.Delivery.Files.Add(deliveryFile);
			//#endregion





			#endregion

			this.ReportProgress(0.9);
			this.Delivery.Save();

			this.ReportProgress(1);

			return ServiceOutcome.Success;
		}

		private string GetMethodUrl(string relativeUrl, Dictionary<string, string> methodParams)
		{

			StringBuilder urlParams = new StringBuilder();
			urlParams.Append(relativeUrl);
			urlParams.Append("?");
			foreach (KeyValuePair<string, string> param in methodParams)
			{
				urlParams.Append(param.Key);
				urlParams.Append("=");
				urlParams.Append(param.Value);
				urlParams.Append("&");
			}
			Uri uri = new Uri(_baseAddress, urlParams.ToString());
			methodParams.Clear();
			return uri.ToString();

		}
		/// <summary>
		/// method for converting a System.DateTime value to a UNIX Timestamp
		/// </summary>
		/// <param name="value">date to convert</param>
		/// <returns></returns>
		private double ConvertToTimestamp(DateTime value)
		{
			DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
			if (value.Millisecond > 0)
				value = value.AddMilliseconds(-value.Millisecond);
			TimeSpan diff = value - origin;
			return Math.Floor(diff.TotalSeconds);
		}
		private string ConvertToFacebookDateTime(DateTime value)
		{
			int timeZone;
			int offset;
			if (!Instance.Configuration.Options.ContainsKey("TimeZone"))
				throw new Exception("Time zone must be configured! for utc please put 0!");
			timeZone = int.Parse(Instance.Configuration.Options["TimeZone"]);
			
			if (!Instance.Configuration.Options.ContainsKey("Offset"))
				throw new Exception("Offset must be configured!for non offset please put 0");
			offset = int.Parse(Instance.Configuration.Options["Offset"]);

			value = value.AddHours(-timeZone);
			value = value.AddHours(offset);
			return value.ToString("yyyy-MM-ddTHH:mm:ss");
		}






	}
}
