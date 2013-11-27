using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Core.Services;

namespace Edge.Services.Currencies
{
    public class YahooFinanceInitializerService : PipelineService
    {

        protected override ServiceOutcome DoPipelineWork()
        {

            this.Delivery = this.NewDelivery(); // setup delivery
            this.Delivery.FileDirectory = Instance.Configuration.Options[Edge.Data.Pipeline.Services.Const.DeliveryServiceConfigurationOptions.FileDirectory];
            this.Delivery.Account = new Account()
            {
                ID = 0
            };

            string fileName = string.Empty;

            //FileName
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["FileName"]))
                throw new Exception("Missing Configuration Param , FileName");
            else
                fileName = this.Instance.Configuration.Options["FileName"];

            DeliveryFile _file = new DeliveryFile()
            {
                Name = fileName
            };

            _file.SourceUrl = Instance.Configuration.Options["SourceUrl"];

            _file.Parameters.Add("XML.IsAttribute", Instance.Configuration.Options["XML.IsAttribute"]);
            _file.Parameters.Add("XML.Path", Instance.Configuration.Options["XML.Path"]);

            this.Delivery.Files.Add(_file);


            //Set Output


            this.Delivery.Outputs.Add(new DeliveryOutput()
            {
                Signature = Delivery.CreateSignature(String.Format("[{0}]-[{1}]",
                    this.TimePeriod.ToAbsolute(),
                    Instance.Configuration.Options["SourceUrl"]
                )),
                Account = new Data.Objects.Account() { ID = 0 },
                TimePeriodStart = Delivery.TimePeriodStart,
                TimePeriodEnd = Delivery.TimePeriodEnd
            });

            this.Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }

    }
}
