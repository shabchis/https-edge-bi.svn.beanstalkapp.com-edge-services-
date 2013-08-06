using System;
using System.Linq;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;

namespace Edge.Services.Google.AdWords.Performance
{
	public class InitializerService : PipelineService
	{
		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			#region Check config parameters
			if (!Configuration.Parameters.ContainsKey("FilterDeleted"))
				throw new Exception("Missing Configuration Param: FilterDeleted");

			if (!Configuration.Parameters.ContainsKey("KeywordContentId"))
				throw new Exception("Missing Configuration Param: KeywordContentId");

			if (!Configuration.Parameters.ContainsKey("DeveloperToken"))
				throw new Exception("Missing Configuration Param: DeveloperToken");

			if (!Configuration.Parameters.ContainsKey("Adwords.MccEmail"))
				throw new Exception("Missing Configuration Param: Adwords.MccEmail");

			if (!Configuration.Parameters.ContainsKey("Adwords.ClientID"))
				throw new Exception("Missing Configuration Param: Adwords.ClientID");

			if (!Configuration.Parameters.ContainsKey("SubChannelName"))
				throw new Exception("Missing Configuration Param: SubChannelName");

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
			Delivery.Parameters["FilterDeleted"] = Configuration.Parameters["FilterDeleted"];
			//Delivery.Parameters["IncludeStatus"] = Configuration.Parameters.Get<string>("IncludeStatus");

			//Get MCC Paramerters
			Delivery.Parameters["DeveloperToken"] = Configuration.Parameters.Get<string>("DeveloperToken");
			Delivery.Parameters["MccEmail"] = Configuration.Parameters.Get<string>("Adwords.MccEmail");
			Delivery.Parameters["KeywordContentId"] = Configuration.Parameters.Get<string>("KeywordContentId");

			//Get Account Client Id's
			var adwordsClientIds = Configuration.Parameters.Get<string>("Adwords.ClientID").Split('|');
			Delivery.Parameters["AdwordsClientIDs"] = adwordsClientIds;

			// Get Reports configuration
			var reportConfig = AdwordsReportConfig.Deserialize(Configuration.Parameters.Get<string>("Adwords.ReportConfig"));

			#endregion

			#region Nice to have params

			//Check for includeZeroImpression
			if (Configuration.Parameters.ContainsKey("includeZeroImpression"))
				Delivery.Parameters["includeZeroImpression"] = Configuration.Parameters.Get<string>("includeZeroImpression");
			else
				Delivery.Parameters["includeZeroImpression"] = false;

			//Check for includeConversionTypes
			if (Configuration.Parameters.ContainsKey("includeConversionTypes"))
				Delivery.Parameters["includeConversionTypes"] = Configuration.Parameters.Get<string>("includeConversionTypes");
			else
				Delivery.Parameters["includeConversionTypes"] = false; // default

			//Check for includeDisplaytData
			if (Configuration.Parameters.ContainsKey("includeDisplaytData"))
				Delivery.Parameters["includeDisplaytData"] = Configuration.Parameters.Get<string>("includeDisplaytData");
			else
				Delivery.Parameters["includeDisplaytData"] = false; // default
			#endregion

			Progress = 0.5;
			// create Delivery files for each Client ID and Report type
			foreach (var clientId in adwordsClientIds)
			{
				foreach (var report in reportConfig.Reports.Where(x => x.Enable))
				{
					var file = new DeliveryFile { Name = report.Name };
					file.Parameters.Add("ReportType", report.Type);
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
