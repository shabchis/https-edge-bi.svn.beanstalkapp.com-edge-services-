using System;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Metrics.Managers;

namespace Edge.Services.BackOffice.Generic
{
    public class InitializerService : PipelineService
    {
		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			#region Init General
			if (!Configuration.TimePeriod.HasValue)
				throw new Exception("No time period is set for Service");

			if (!Configuration.Parameters.ContainsKey("AccountID"))
				throw new Exception("Missing Configuration Param: AccountID");

			if (!Configuration.Parameters.ContainsKey(Const.DeliveryServiceConfigurationOptions.FileDirectory))
				throw new Exception(String.Format("Missing Configuration Param: {0}", Const.DeliveryServiceConfigurationOptions.FileDirectory));

			if (!Configuration.Parameters.ContainsKey(BoConfigurationOptions.BaseServiceAddress))
				throw new Exception(String.Format("Missing Configuration Param: {0}", BoConfigurationOptions.BaseServiceAddress));

			var accountId = Configuration.Parameters.Get("AccountID", false, -1);
			var channelId = Configuration.Parameters.Get("ChannelID", false, -1);

			Delivery = NewDelivery();
			Delivery.Account = new Account { ID = accountId };
			Delivery.Channel = new Channel { ID = channelId };
			Delivery.TimePeriodDefinition = Configuration.TimePeriod.Value;
			Delivery.FileDirectory = Configuration.Parameters.Get<string>(Const.DeliveryServiceConfigurationOptions.FileDirectory);

			// Create an import manager that will handle rollback, if necessary
			HandleConflicts(new MetricsDeliveryManager(InstanceID), DeliveryConflictBehavior.Abort);

			var baseAddress = Configuration.Parameters.Get<string>(BoConfigurationOptions.BaseServiceAddress);

			int utcOffset = 0;
			if (Configuration.Parameters.ContainsKey(BoConfigurationOptions.UtcOffset))
				utcOffset = Configuration.Parameters.Get<int>(BoConfigurationOptions.UtcOffset);
			Delivery.Parameters.Add(BoConfigurationOptions.UtcOffset, utcOffset);

			int timeZone = 0;
			if (Configuration.Parameters.ContainsKey(BoConfigurationOptions.TimeZone))
				timeZone = Configuration.Parameters.Get<int>(BoConfigurationOptions.TimeZone);
			Delivery.Parameters.Add(BoConfigurationOptions.TimeZone, timeZone);

			Progress = 0.2;
			#endregion

			#region DeliveryFile
			var boFile = new DeliveryFile();
			DateTime start = Delivery.TimePeriodDefinition.Start.ToDateTime();
			DateTime end = Delivery.TimePeriodDefinition.End.ToDateTime();
			boFile.Parameters[BoConfigurationOptions.BO_XPath_Trackers] = Configuration.Parameters.Get<string>(BoConfigurationOptions.BO_XPath_Trackers);
			boFile.Name = BoConfigurationOptions.BoFileName;
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
			boFile.SourceUrl = string.Format(baseAddress, start, end);
			boFile.Parameters.Add(BoConfigurationOptions.IsAttribute, Configuration.Parameters.Get<bool>(BoConfigurationOptions.IsAttribute));
			boFile.Parameters.Add(BoConfigurationOptions.TrackerFieldName, Configuration.Parameters.Get<string>(BoConfigurationOptions.TrackerFieldName));

			Delivery.Files.Add(boFile);
			Delivery.Save();

			#endregion

			return ServiceOutcome.Success;
		} 
		#endregion
    }
}
