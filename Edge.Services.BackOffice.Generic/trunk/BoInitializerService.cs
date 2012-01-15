using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Services.AdMetrics;


namespace Edge.Services.BackOffice.Generic
{
	class BoInitializerService : PipelineService
	{
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			string baseAddress;
			#region Init General
			// ...............................
			// SETUP
			this.Delivery = this.NewDelivery();

			// This is for finding conflicting services
			this.Delivery.Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]",
				this.Instance.AccountID,				
				this.TargetPeriod.ToAbsolute()));

			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new Edge.Data.Pipeline.Common.Importing.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[Edge.Data.Pipeline.Common.Importing.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			// ...............................

			// Now that we have a new delivery, start adding values
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				
			};
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = -1
			};

			this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			if (string.IsNullOrEmpty(this.Delivery.TargetLocationDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");

			if (string.IsNullOrEmpty(this.Instance.Configuration.Options[BoConfigurationOptions.BaseServiceAddress]))
				throw new Exception("base url must be configured!");
			baseAddress =this.Instance.Configuration.Options[BoConfigurationOptions.BaseServiceAddress];

			if (string.IsNullOrEmpty(this.Instance.Configuration.Options[BoConfigurationOptions.UserName]))
				throw new Exception("base url must be configured!");
			this.Delivery.Parameters["UserName"] = this.Instance.Configuration.Options[BoConfigurationOptions.UserName];

			if (string.IsNullOrEmpty(this.Instance.Configuration.Options[BoConfigurationOptions.Password]))
				throw new Exception("base url must be configured!");
			this.Delivery.Parameters["Password"] = this.Instance.Configuration.Options[BoConfigurationOptions.Password];

			int utcOffset=0;
			if (!string.IsNullOrEmpty(this.Instance.Configuration.Options[BoConfigurationOptions.UtcOffset]))
				utcOffset = int.Parse(this.Instance.Configuration.Options[BoConfigurationOptions.UtcOffset]);

			
			this.ReportProgress(0.2);
			#endregion

			#region DeliveryFile
			Dictionary<string, string> UrlParams = new Dictionary<string, string>();
			DeliveryFile boFile = new DeliveryFile();
			boFile.Parameters[BoConfigurationOptions.BO_XPath_Trackers] = Instance.Configuration.Options[BoConfigurationOptions.BO_XPath_Trackers];
			boFile.Name = BoConfigurationOptions.BoFileName;
			UrlParams.Add("from", utcOffset==0 ? Delivery.TargetPeriod.Start.ToDateTime().ToString("yyyy-MM-ddTHH:MMZ") : ConvertToTimeZone(utcOffset,TargetPeriod.Start.ToDateTime()));
			UrlParams.Add("to",utcOffset==0 ? Delivery.TargetPeriod.End.ToDateTime().ToString("yyyy-MM-ddTHH:MMZ") : ConvertToTimeZone(utcOffset,TargetPeriod.Start.ToDateTime()));
			UrlParams.Add("vendor",this.Delivery.Parameters["UserName"].ToString());
			UrlParams.Add("Password", this.Delivery.Parameters["Password"].ToString());
			boFile.SourceUrl = CreateUrl(UrlParams, baseAddress.ToString());

			this.Delivery.Files.Add(boFile);
			this.Delivery.Save();


			#endregion
			return Core.Services.ServiceOutcome.Success;
		}

		private string CreateUrl(Dictionary<string, string> UrlParams, string baseAddress)
		{
			StringBuilder fullAddress = new StringBuilder();
			fullAddress.Append(baseAddress);
			fullAddress.Append("?");
			foreach (KeyValuePair<string, string> param in UrlParams)
			{
				fullAddress.Append(param.Key);
				fullAddress.Append("=");
				fullAddress.Append(param.Value);
				fullAddress.Append("&");
			}


			return fullAddress.ToString();
		}

		private string ConvertToTimeZone(int utcOffset, DateTime dateTime)
		{
			throw new NotImplementedException();
		}

		
	}
}
