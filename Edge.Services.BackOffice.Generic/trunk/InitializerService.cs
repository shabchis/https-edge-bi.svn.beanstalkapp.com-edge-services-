using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Metrics.GenericMetrics;



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

			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,

			};
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = -1
			};

			this.Delivery.TimePeriodDefinition = this.TimePeriod;

			this.Delivery.FileDirectory = Instance.Configuration.Options[Const.DeliveryServiceConfigurationOptions.FileDirectory];

			if (string.IsNullOrEmpty(this.Delivery.FileDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");

            bool multibo = false;
            if (this.Instance.Configuration.Options.ContainsKey("URL.SignatureAppend"))
                multibo = Convert.ToBoolean(this.Instance.Configuration.Options["URL.SignatureAppend"]);

			this.Delivery.Outputs.Add(new DeliveryOutput()
			{
				Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}{2}]", this.Instance.AccountID, this.TimePeriod.ToAbsolute(),multibo?"-"+this.Instance.Configuration.Options["Bo.ServiceAdress"]:string.Empty)),
				Account = Delivery.Account,
				Channel = Delivery.Channel,
				TimePeriodStart = Delivery.TimePeriodStart,
				TimePeriodEnd = Delivery.TimePeriodEnd


			});

			// Create an import manager that will handle rollback, if necessary
			var importManager = new GenericMetricsImportManager(this.Instance.InstanceID, new Edge.Data.Pipeline.Common.Importing.MetricsImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[Edge.Data.Pipeline.Metrics.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);
          

            

         

            if (string.IsNullOrEmpty(this.Instance.Configuration.Options[BoConfigurationOptions.BaseServiceAddress]))
                throw new Exception("base url must be configured!");
            baseAddress = this.Instance.Configuration.Options[BoConfigurationOptions.BaseServiceAddress];

            

            int utcOffset = 0;
            if (this.Instance.Configuration.Options.ContainsKey(BoConfigurationOptions.UtcOffset))
                utcOffset = int.Parse(this.Instance.Configuration.Options[BoConfigurationOptions.UtcOffset]);
			Delivery.Parameters.Add(BoConfigurationOptions.UtcOffset, utcOffset);
			int timeZone=0;
			if (Instance.Configuration.Options.ContainsKey(BoConfigurationOptions.TimeZone))
				timeZone = int.Parse(Instance.Configuration.Options[BoConfigurationOptions.TimeZone]);
			Delivery.Parameters.Add(BoConfigurationOptions.TimeZone, timeZone);
            this.ReportProgress(0.2);
            #endregion

            #region DeliveryFile
            Dictionary<string, string> UrlParams = new Dictionary<string, string>();
            DeliveryFile boFile = new DeliveryFile();
			DateTime start=TimePeriod.Start.ToDateTime();
			DateTime end = TimePeriod.End.ToDateTime();
            boFile.Parameters[BoConfigurationOptions.BO_XPath_Trackers] = Instance.Configuration.Options[BoConfigurationOptions.BO_XPath_Trackers];
            boFile.Name = BoConfigurationOptions.BoFileName;
			if (utcOffset != 0)
			{
				start=start.AddHours(utcOffset);
				end = end.AddHours(utcOffset);				
			}
			if (timeZone != 0)
			{
				start = start.AddHours(-timeZone);
				end = end.AddHours(-timeZone);
			}




			boFile.SourceUrl = string.Format(baseAddress, start, end);
            

            boFile.Parameters.Add(BoConfigurationOptions.IsAttribute, Instance.Configuration.Options[BoConfigurationOptions.IsAttribute]);
            boFile.Parameters.Add(BoConfigurationOptions.TrackerFieldName, Instance.Configuration.Options[BoConfigurationOptions.TrackerFieldName]);


            this.Delivery.Files.Add(boFile);
            this.Delivery.Save();


            #endregion
            return Core.Services.ServiceOutcome.Success;
        }


       


    }
}
