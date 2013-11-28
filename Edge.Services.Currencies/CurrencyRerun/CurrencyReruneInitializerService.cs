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
    public class CurrencyRerunInitializerService : PipelineService
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


            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["CurrencyCodes"]))
                throw new Exception("Missing Configuration Param , CurrencyCodes");
            else
                this.Delivery.Parameters.Add("CurrencyCodes", Instance.Configuration.Options["CurrencyCodes"]);


            foreach (string currencyCode in   this.Delivery.Parameters["CurrencyCodes"].ToString().Split(';'))
            {
                //http://currencies.apps.grandtrunk.net/getrange/2013-11-25/2013-11-26/HKD/USD
                 _file.SourceUrl = string.Format("{3}{0}/{1}/{2}/USD",
                    this.Delivery.TimePeriodDefinition.Start.ToDateTime().AddDays(-1).ToString("yyyy-MM-dd"),
                    this.Delivery.TimePeriodDefinition.Start.ToDateTime().ToString("yyyy-MM-dd"),
                    currencyCode,
                    Instance.Configuration.Options["SourceUrl"]
                    );
                 _file.Parameters.Add("CurrencyCode", currencyCode);

                this.Delivery.Files.Add(_file);
            }

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
