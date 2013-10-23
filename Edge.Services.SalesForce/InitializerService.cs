using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Metrics.GenericMetrics;
using System.Text.RegularExpressions;

namespace Edge.Services.SalesForce
{
	public class InitializerService : PipelineService
	{
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			
			#region Init General
			
			// ...............................
			// SETUP
			this.Delivery = this.NewDelivery();
			this.Delivery.TimePeriodDefinition = this.TimePeriod;

			this.Delivery.FileDirectory = Instance.Configuration.Options[Const.DeliveryServiceConfigurationOptions.FileDirectory];
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				

			};
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = -1
			};

			if (string.IsNullOrEmpty(this.Delivery.FileDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");

			if (!this.Instance.Configuration.Options.ContainsKey("AuthenticationUrl"))
				throw new Exception("AuthenticationUrl must be configured in configuration file");
			this.Delivery.Parameters["AuthenticationUrl"] = this.Instance.Configuration.Options["AuthenticationUrl"];//https://test.salesforce.com/services/oauth2/token

            if (!this.Instance.Configuration.Options.ContainsKey("ConsumerKey"))
				throw new Exception("ClientID must be configured in configuration file");
            this.Delivery.Parameters["SalesForceClientID"] = this.Instance.Configuration.Options["ConsumerKey"];//3MVG9GiqKapCZBwG.OqpT.DCgHmIXOlszzCpZPxbRyvzPDNlshB5LD0x94rQO5SzGOAZrWPNIPm_aGR7nBeXe

			if (!this.Instance.Configuration.Options.ContainsKey("ConsentCode"))
				throw new Exception("ConsentCode must be configured in configuration file");
			this.Delivery.Parameters["ConsentCode"] = this.Instance.Configuration.Options["ConsentCode"];//(accesstoken
           
            if (!this.Instance.Configuration.Options.ContainsKey("ConsumerSecret"))
				throw new Exception("ClientSecret must be configured in configuration file");
            this.Delivery.Parameters["ClientSecret"] = this.Instance.Configuration.Options["ConsumerSecret"];//321506373515061074

			if (!this.Instance.Configuration.Options.ContainsKey("Redirect_URI"))
				throw new Exception("Redirect_URI must be configured in configuration file");
			this.Delivery.Parameters["Redirect_URI"] = this.Instance.Configuration.Options["Redirect_URI"];//http://localhost:8080/RestTest/oauth/_callback

			// This is for finding conflicting services
			this.Delivery.Outputs.Add(new DeliveryOutput()
			{
				Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]-[{2}]",
			  this.Instance.AccountID,
			  this.TimePeriod.ToAbsolute(), this.Delivery.Parameters["SalesForceClientID"])),
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

			// ...............................
			// Now that we have a new delivery, start adding values

			int utcOffset = 0;
			if (this.Instance.Configuration.Options.ContainsKey(BoConfigurationOptions.UtcOffset))
				utcOffset = int.Parse(this.Instance.Configuration.Options[BoConfigurationOptions.UtcOffset]);
			Delivery.Parameters.Add(BoConfigurationOptions.UtcOffset, utcOffset);
			int timeZone = 0;
			if (Instance.Configuration.Options.ContainsKey(BoConfigurationOptions.TimeZone))
				timeZone = int.Parse(Instance.Configuration.Options[BoConfigurationOptions.TimeZone]);
			Delivery.Parameters.Add(BoConfigurationOptions.TimeZone, timeZone);
			this.ReportProgress(0.2);
			//if (!Instance.Configuration.Options.ContainsKey("ServiceAddress"))
			//    throw new Exception("ServiceAddress must be configured in configuration file");
			//serviceAddress = Instance.Configuration.Options["ServiceAddress"]; 

			#endregion

			#region DeliveryFile
            string query = Instance.Configuration.Options["Query"];
            //@SMBLeads:SELECT  CreatedDate,Edge_BI_Tracker__c ,Company_Type__c  FROM Lead WHERE CreatedDate > 2013-06-28T00:00:00.00Z AND Edge_BI_Tracker__c != null

            string[] queries = query.Split('@');
           
            foreach (string qwry in queries)
            {
                DeliveryFile file = new DeliveryFile();
                string[] q = qwry.Split(':');
                file.Name = q[0]+"-0";
                DateTime start = TimePeriod.Start.ToDateTime();
                DateTime end = TimePeriod.End.ToDateTime();
                if (utcOffset != 0)
                {
                    start = start.AddHours(utcOffset);
                    end = end.AddHours(utcOffset);
                }
                if (timeZone != 0)
                {
                    start = start.AddHours(-timeZone);
                    end = end.AddHours(-timeZone);
                }
                file.Parameters["Query"] = q[1];
                
                this.Delivery.Files.Add(file);
            }

			
			this.Delivery.Save();

			return Core.Services.ServiceOutcome.Success;

			#endregion
		}
		
	}
}
