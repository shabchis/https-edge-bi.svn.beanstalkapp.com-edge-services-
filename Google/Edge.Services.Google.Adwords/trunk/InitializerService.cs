using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;

namespace Edge.Services.Google.Adwords
{
    public class InitializerService: PipelineService
    {
        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            this.Delivery = new Delivery(Instance.InstanceID);
            this.Delivery.TargetPeriod = this.TargetPeriod;

            this.Delivery.Parameters["AccountID"] = Instance.AccountID;

            //Instance.Configuration.Options["Adwords.Email"]

            this.Delivery.Files.Add(new DeliveryFile()
            {
                Name = "AdwordsCreativeReport",
                SourceUrl = url // TODO: get from API
            });

            this.Delivery.Save();

            return Core.Services.ServiceOutcome.Success;

        }
    }
}
