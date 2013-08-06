using System;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Metrics.Services.Configuration;
using Edge.Data.Pipeline.Services;
using Edge.Services.BackOffice.Generic;

namespace Edge.Services.SalesForce
{
	public class InitializerService : PipelineService
	{
		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			if (!Configuration.TimePeriod.HasValue)
				throw new Exception("No time period is set for Service");

			if (!Configuration.Parameters.ContainsKey(Const.DeliveryServiceConfigurationOptions.FileDirectory))
				throw new Exception(String.Format("Missing Configuration Param: {0}", Const.DeliveryServiceConfigurationOptions.FileDirectory));

			if (!(Configuration is AutoMetricsProcessorServiceConfiguration))
				throw new Exception("Service should have AutoMetricsProcessorServiceConfiguration");
			var config = Configuration as AutoMetricsProcessorServiceConfiguration;

			var accountId = Configuration.Parameters.Get("AccountID", false, -1);
			var channelId = Configuration.Parameters.Get("ChannelID", false, -1);

			// create delivery
			Delivery = NewDelivery();
			Delivery.Account = new Account { ID = accountId };
			Delivery.Channel = new Channel { ID = channelId };
			Delivery.TimePeriodDefinition = Configuration.TimePeriod.Value;
			Delivery.FileDirectory = Configuration.Parameters.Get<string>(Const.DeliveryServiceConfigurationOptions.FileDirectory);

			// must parameters
			if (string.IsNullOrEmpty(Delivery.FileDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");

			if (!Configuration.Parameters.ContainsKey("AuthenticationUrl"))
				throw new Exception("AuthenticationUrl must be configured in configuration file");
			Delivery.Parameters["AuthenticationUrl"] = Configuration.Parameters.Get<string>("AuthenticationUrl"); //https://test.salesforce.com/services/oauth2/token

			if (!Configuration.Parameters.ContainsKey("SalesForceClientID"))
				throw new Exception("ClientID must be configured in configuration file");
			Delivery.Parameters["SalesForceClientID"] = Configuration.Parameters.Get<string>("SalesForceClientID"); //3MVG9GiqKapCZBwG.OqpT.DCgHmIXOlszzCpZPxbRyvzPDNlshB5LD0x94rQO5SzGOAZrWPNIPm_aGR7nBeXe

			if (!Configuration.Parameters.ContainsKey("ConsentCode"))
				throw new Exception("ConsentCode must be configured in configuration file");
			Delivery.Parameters["ConsentCode"] = Configuration.Parameters.Get<string>("ConsentCode"); //(accesstoken

			if (!Configuration.Parameters.ContainsKey("ClientSecret"))
				throw new Exception("ClientSecret must be configured in configuration file");
			Delivery.Parameters["ClientSecret"] = Configuration.Parameters.Get<string>("ClientSecret"); //321506373515061074

			if (!Configuration.Parameters.ContainsKey("Redirect_URI"))
				throw new Exception("Redirect_URI must be configured in configuration file");
			Delivery.Parameters["Redirect_URI"] = Configuration.Parameters.Get<string>("Redirect_URI"); //http://localhost:8080/RestTest/oauth/_callback

			// Create an import manager that will handle rollback, if necessary
			HandleConflicts(new MetricsDeliveryManager(InstanceID), DeliveryConflictBehavior.Abort);

			// optional parameters
			var utcOffset = 0;
			if (Configuration.Parameters.ContainsKey(BoConfigurationOptions.UtcOffset))
				utcOffset = Configuration.Parameters.Get<int>(BoConfigurationOptions.UtcOffset);
			Delivery.Parameters.Add(BoConfigurationOptions.UtcOffset, utcOffset);

			var timeZone = 0;
			if (Configuration.Parameters.ContainsKey(BoConfigurationOptions.TimeZone))
				timeZone = Configuration.Parameters.Get<int>(BoConfigurationOptions.TimeZone);
			Delivery.Parameters.Add(BoConfigurationOptions.TimeZone, timeZone);

			Progress = 0.2;

			// delivery file
			var file = new DeliveryFile { Name = config.DeliveryFileName };

			// TODO: what are these dates for? May be query parameters?
			//var start = Delivery.TimePeriodDefinition.Start.ToDateTime();
			//var end = Delivery.TimePeriodDefinition.End.ToDateTime();
			//if (utcOffset != 0)
			//{
			//	start = start.AddHours(utcOffset);
			//	end = end.AddHours(utcOffset);
			//}
			//if (timeZone != 0)
			//{
			//	start = start.AddHours(-timeZone);
			//	end = end.AddHours(-timeZone);
			//}
			if (!Configuration.Parameters.ContainsKey("Query"))
				throw new Exception("Query must be configured in configuration file for SaleForce service");
			file.Parameters["Query"] = Configuration.Parameters.Get<string>("Query");

			Delivery.Files.Add(file);
			Delivery.Save();

			return ServiceOutcome.Success;
		} 
		#endregion
	}
}
