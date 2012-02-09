using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Services.SegmentMetrics;


namespace Edge.Services.BackOffice.Generic
{
    class InitializerService : PipelineService
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
            var importManager = new SegmentMetricsImportManager(this.Instance.InstanceID, new Edge.Data.Pipeline.Common.Importing.ImportManagerOptions()
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
            baseAddress = this.Instance.Configuration.Options[BoConfigurationOptions.BaseServiceAddress];

            

            int utcOffset = 0;
            if (!string.IsNullOrEmpty(this.Instance.Configuration.Options[BoConfigurationOptions.UtcOffset]))
                utcOffset = int.Parse(this.Instance.Configuration.Options[BoConfigurationOptions.UtcOffset]);
			Delivery.Parameters.Add(BoConfigurationOptions.UtcOffset, utcOffset);
			int timeZone=0;
			if (!Instance.Configuration.Options.ContainsKey(BoConfigurationOptions.TimeZone))
				timeZone = int.Parse(Instance.Configuration.Options[BoConfigurationOptions.TimeZone]);
			Delivery.Parameters.Add(BoConfigurationOptions.TimeZone, timeZone);
            this.ReportProgress(0.2);
            #endregion

            #region DeliveryFile
            Dictionary<string, string> UrlParams = new Dictionary<string, string>();
            DeliveryFile boFile = new DeliveryFile();
			DateTime start=TargetPeriod.Start.ToDateTime();
			DateTime end = TargetPeriod.End.ToDateTime();
            boFile.Parameters[BoConfigurationOptions.BO_XPath_Trackers] = Instance.Configuration.Options[BoConfigurationOptions.BO_XPath_Trackers];
            boFile.Name = BoConfigurationOptions.BoFileName;
			if (utcOffset != 0)
			{
				start.AddHours(utcOffset);
				end.AddHours(utcOffset);				
			}
			if (timeZone != 0)
			{
				start.AddHours(-timeZone);
				end.AddHours(-timeZone);
			}




			boFile.SourceUrl = string.Format(baseAddress, start, end);
            

            boFile.Parameters.Add(BoConfigurationOptions.IsAttribute, Instance.Configuration.Options[BoConfigurationOptions.IsAttribute]);
            boFile.Parameters.Add(BoConfigurationOptions.TrackerFieldName, Instance.Configuration.Options[BoConfigurationOptions.TrackerFieldName]);


            this.Delivery.Files.Add(boFile);
            this.Delivery.Save();


            #endregion
            return Core.Services.ServiceOutcome.Success;
        }

		//private string CreateUrl(Dictionary<string, string> UrlParams, string baseAddress)
		//{
		//    StringBuilder fullAddress = new StringBuilder();
		//    fullAddress.Append(baseAddress);
		//    fullAddress.Append("?");
		//    foreach (KeyValuePair<string, string> param in UrlParams)
		//    {
		//        fullAddress.Append(param.Key);
		//        fullAddress.Append("=");
		//        fullAddress.Append(param.Value);
		//        fullAddress.Append("&");
		//    }


		//    return fullAddress.ToString();
		//}

       


    }
}
