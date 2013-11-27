using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using System.Text.RegularExpressions;

namespace Edge.Services.Currencies
{
    public class YahooFinanceProcessorService : PipelineService
    {
        //public CurrencyImportManager ImportManager;

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            List<CurrencyRate> rates = new List<CurrencyRate>();
            //TO DO : USE MAPPING CONFIGURATION FOR THIS SERVICE.

            foreach (DeliveryFile ReportFile in this.Delivery.Files)
            {
                bool isAttribute = Boolean.Parse(ReportFile.Parameters["XML.IsAttribute"].ToString());
                var ReportReader = new XmlDynamicReader
                    (ReportFile.OpenContents(), ReportFile.Parameters["XML.Path"].ToString());


                using (ReportReader)
                {
                    dynamic reader;

                    while (ReportReader.Read())
                    {
                        if (isAttribute)
                            reader = ReportReader.Current.Attributes;
                        else
                            reader = ReportReader.Current;

                        CurrencyRate currencyUnit = new CurrencyRate();

                        //Currency Code
                        List<object> CurrencyData = reader["field"];

                        currencyUnit.Currency.Code = (((XmlDynamicObject)CurrencyData[0]).InnerText.Split('/')).Count() > 1 ?((XmlDynamicObject)CurrencyData[0]).InnerText.Split('/')[1]: string.Empty;

                        if (string.IsNullOrEmpty(currencyUnit.Currency.Code)) continue;

                        //Currecy Date
                        currencyUnit.RateDate = DateTime.Now;

                        //Currency Rate
                        currencyUnit.RateValue =Convert.ToDecimal(((XmlDynamicObject)CurrencyData[1]).InnerText)==0?0: 1/Convert.ToDecimal(((XmlDynamicObject)CurrencyData[1]).InnerText);
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
