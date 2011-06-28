using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceModel;
using System.Text;
using Edge.Core;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;

namespace Edge.Services.Microsoft.AdCenter
{
	public class InitializerService : PipelineService
	{

		protected override ServiceOutcome DoPipelineWork()
		{
			// Create a new delivery
			this.Delivery = new Delivery(this.Instance.InstanceID, this.DeliveryID)
			{
				TargetPeriod = this.TargetPeriod,
				TargetLocationDirectory = "Microsoft"
			};

			// AccountID as parameter for entire delivery
			this.Delivery.Account = new Data.Objects.Account() { ID = this.Instance.AccountID, OriginalID=Instance.Configuration.Options["AdCenter.CustomerAccountID"] }; //TODO: ASK DORON ORIGINAL ID? 
			this.Delivery.Channel = new Channel() { ID = 14 };

			// Wrapper for adCenter API
			AdCenterApi adCenterApi = new AdCenterApi(this);

			// ................................
			// Campaign report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.CampaignReport });

			ReportProgress(0.33);

			// ................................
			// Ad report


			ReportProgress(0.33);

			// ................................
			// Keyword report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.KeywordReport });



			this.Delivery.Files[Const.Files.KeywordReport].Parameters.Add(Const.Parameters.TimePeriodColumnName, AdCenterApi.GetTimePeriodColumnName(WS.ReportAggregation.Daily));


			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.AdReport });
			ReportProgress(0.33);

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

	}
}
