using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;

namespace Edge.Services.Currencies
{
    public class InitializerService : PipelineService
    {

        protected override Edge.Core.Services.ServiceOutcome DoPipelineWork()
        {
            this.Delivery = this.NewDelivery(); // setup delivery

            string fileName = string.Empty;

            //FileName
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["FileName"]))
                throw new Exception("Missing Configuration Param , FileName");
            else
                fileName = this.Instance.Configuration.Options["FileName"];

            //CrossRateSymbols
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["CrossRateSymbols"]))
                throw new Exception("Missing Configuration Param , CrossRateSymbols");
            else
                this.Delivery.Parameters.Add("CrossRateSymbols", this.Instance.Configuration.Options["CrossRateSymbols"]);

            //UserName
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["UserName"]))
                throw new Exception("Missing Configuration Param , UserName");
            else
                this.Delivery.Parameters.Add("UserName", this.Instance.Configuration.Options["UserName"]);

            //UserPassword
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["UserPassword"]))
                throw new Exception("Missing Configuration Param , UserPassword");
            else
                this.Delivery.Parameters.Add("UserPassword", this.Instance.Configuration.Options["UserPassword"]);


            DeliveryFile _file = new DeliveryFile()
            {
                Name = fileName
            };

            _file.SourceUrl = Instance.Configuration.Options["SourceUrl"];// "http://www.xignite.com/xCurrencies.asmx";
            _file.Parameters.Add("Content-Type", "text/xml; charset=utf-8");
            _file.Parameters.Add("SOAPAction", Instance.Configuration.Options["SOAPAction"]);
            _file.Parameters.Add("SoapMethod", Instance.Configuration.Options["SoapMethod"]);
            
            this.Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }
    }
}
