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
    public class ProcessorService : PipelineService
    {
        //public CurrencyImportManager ImportManager;

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            List<CurrencyRate> rates = new List<CurrencyRate>();
            //TO DO : USE MAPPING CONFIGURATION FOR THIS SERVICE.

            //ImportManager = new CurrencyImportManager(this.Instance.InstanceID, null);

            foreach (DeliveryFile ReportFile in this.Delivery.Files)
            {
                bool isAttribute = Boolean.Parse(ReportFile.Parameters["XML.IsAttribute"].ToString());
                var ReportReader = new XmlDynamicReader
                    (ReportFile.OpenContents(), ReportFile.Parameters["XML.Path"].ToString());

                //using (ImportManager)
                //{
                //    ImportManager.BeginImport(this.Delivery);

                //DeliveryOutput currentOutput = Delivery.Outputs.First();

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
                        currencyUnit.Currency.Code = Regex.Replace(Convert.ToString(reader["Symbol"]), "USD", string.Empty);

                        //Currecy Date
                        string[] date = (reader["Date"] as string).Split('/');
                        try { currencyUnit.RateDate = new DateTime(Int32.Parse(date[2]), Int32.Parse(date[0]), Int32.Parse(date[1])); }
                        catch (Exception ex)
                        {
                            throw new Exception(String.Format("Could not parse the date parts (y = '{0}', m = '{1}', d = '{2}'.", date[2], date[0], date[1]), ex);
                        }

                        //Currency Rate
                        double rate = Convert.ToDouble(reader["Last"]);
                        currencyUnit.RateValue = Convert.ToDecimal(rate.ToString("#0.0000"));

                        ////Output

                        //currencyUnit.Output = new DeliveryOutput()
                        //{
                        //    Signature = string.Format("{0}_{1}", currencyUnit.Currency.Code, currencyUnit.RateDate),
                        //    TimePeriodStart = currencyUnit.DateCreated,
                        //    TimePeriodEnd = currencyUnit.DateCreated,
                        //};
                        //this.Delivery.Outputs.Add(currencyUnit.Output);

                        //ImportManager.ImportCurrency(currencyUnit);

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
