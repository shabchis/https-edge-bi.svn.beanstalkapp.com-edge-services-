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
	public class AdCenterManager : DeliveryManager
	{

		public override void ApplyUniqueness(Delivery delivery)
		{
			delivery.TargetPeriod = CurrentService.TargetPeriod;
			delivery.Account = new Data.Objects.Account() { ID = CurrentService.Instance.AccountID, OriginalID = CurrentService.Instance.Configuration.Options["AdCenter.CustomerAccountID"] }; //TODO: ASK DORON ORIGINAL ID? 
			delivery.Channel = new Channel() { ID = 14 };
		}
	}

	public class InitializerService : BaseInitializerService
	{
		public override DeliveryManager GetDeliveryManager()
		{
			throw new NotImplementedException();
		}

		public override void ApplyDeliveryDetails()
		{
			// Wrapper for adCenter API
			AdCenterApi adCenterApi = new AdCenterApi(this);
			this.Delivery.TargetLocationDirectory = "Microsoft";

			// ................................
			// Campaign report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.CampaignReport });

			ReportProgress(0.33);

			// ................................
			// Ad report


			ReportProgress(0.66);

			// ................................
			// Keyword report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.KeywordReport });



			this.Delivery.Files[Const.Files.KeywordReport].Parameters.Add(Const.Parameters.TimePeriodColumnName, AdCenterApi.GetTimePeriodColumnName(WS.ReportAggregation.Daily));


			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.AdReport });
			ReportProgress(0.99);
			this.Delivery.Save();
			ReportProgress(1);
		}

		


		
	}
}
