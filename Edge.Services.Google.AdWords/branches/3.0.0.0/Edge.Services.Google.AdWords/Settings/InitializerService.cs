using System;
using System.Linq;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;

namespace Edge.Services.Google.AdWords.Settings
{
	public class InitializerService : PipelineService
	{
		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			#region Check config parameters
			if (!Configuration.Parameters.ContainsKey("DeveloperToken"))
				throw new Exception("Missing Configuration Param: DeveloperToken");

			if (!Configuration.Parameters.ContainsKey("Adwords.MccEmail"))
				throw new Exception("Missing Configuration Param: Adwords.MccEmail");

			if (!Configuration.Parameters.ContainsKey("Adwords.ClientID"))
				throw new Exception("Missing Configuration Param: Adwords.ClientID");

			if (!Configuration.TimePeriod.HasValue)
				throw new Exception("No time period is set for Service");

			if (!Configuration.Parameters.ContainsKey("Adwords.ReportConfig"))
				throw new Exception("Missing Configuration Param: Adwords.ReportConfig");
			#endregion

			var accountId = Configuration.Parameters.Get("AccountID", false, -1);
			var channelId = Configuration.Parameters.Get("ChannelID", false, -1);

			// create Delivery
			if (Delivery == null)
			{
				Delivery = NewDelivery();
				Delivery.TimePeriodDefinition = Configuration.TimePeriod.Value;
				Delivery.Account = accountId != -1 ? new Account { ID = accountId } : null;
				Delivery.FileDirectory = Configuration.Parameters.Get<string>(Const.DeliveryServiceConfigurationOptions.FileDirectory);
				if (channelId != -1) Delivery.Channel = new Channel { ID = channelId };
			}
			Progress = 0.3;

			#region Must Have Params
			//Get MCC Paramerters
			var adwordsClientIds = Configuration.Parameters.Get<string>("Adwords.ClientID").Split('|');
			Delivery.Parameters["AdwordsClientIDs"] = adwordsClientIds;
			Delivery.Parameters["DeveloperToken"] = Configuration.Parameters.Get<string>("DeveloperToken");
			Delivery.Parameters["MccEmail"] = Configuration.Parameters.Get<string>("Adwords.MccEmail");

			// Get Reports configuration
			var reportConfig = AdwordsReportConfig.Deserialize(Configuration.Parameters.Get<string>("Adwords.ReportConfig"));

			#endregion

			Progress = 0.5;
			// create Delivery files for each Client ID and Report type
			foreach (var clientId in adwordsClientIds)
			{
				foreach (var report in reportConfig.Reports.Where(x => x.Enable))
				{
					var file = new DeliveryFile { Name = report.Name };
					file.Parameters.Add("ReportType", report.Type);
					file.Parameters.Add("ReportFilter", report.Filter);
					file.Parameters.Add("ReportFields", report.GetFieldList());
					file.Parameters.Add("AdwordsClientID", clientId);
					Delivery.Files.Add(file);
				}
			}

			Progress = 0.8;
			Delivery.Save();
			return ServiceOutcome.Success;
		} 
		#endregion
	}
}
