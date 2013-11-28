using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using System.Text.RegularExpressions;
using System.IO;

namespace Edge.Services.Currencies
{
    public class CurrencyRerunProcessorService : PipelineService
    {
        //public CurrencyImportManager ImportManager;

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            List<CurrencyRate> rates = new List<CurrencyRate>();
            //TO DO : USE MAPPING CONFIGURATION FOR THIS SERVICE.

            foreach (DeliveryFile ReportFile in this.Delivery.Files)
            {
                using (var fs = new FileStream(ReportFile.Location, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var reader = new StreamReader(fs))
                {
                    while (!reader.EndOfStream)
                    {

                        string line = reader.ReadLine();
                        if (!line.Contains(this.Delivery.TimePeriodDefinition.Start.ToDateTime().ToString("yyyy-MM-dd"))) continue;

                         
                        CurrencyRate currencyUnit = new CurrencyRate();
                        //Currency Code
                        currencyUnit.Currency.Code = ReportFile.Parameters["CurrencyCode"].ToString();
                       
                        //Currecy Date
                        currencyUnit.RateDate = this.Delivery.TimePeriodDefinition.Start.ToDateTime();

                        //Currency Rate
                        currencyUnit.RateValue = Convert.ToDecimal((line.Split(' '))[1].Trim());
                        rates.Add(currencyUnit);
                    }
                }

                CurrencyRate.SaveCurrencyRates(rates);

                //ImportManager.EndImport();
                //}
            }
            return Core.Services.ServiceOutcome.Success;
        }
    }
}
