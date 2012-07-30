using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;

namespace Edge.Services.Currencies
{
	public class ProcessorService : PipelineService
	{
		public new CurrencyImportManager ImportManager;
		
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
		
			//TO DO : USE MAPPING CONFIGURATION FOR THIS SERVICE.

			foreach (DeliveryFile ReportFile in this.Delivery.Files)
			{
				bool isAttribute = Boolean.Parse(ReportFile.Parameters["XML.IsAttribute"].ToString());
				var ReportReader = new XmlDynamicReader
					(ReportFile.OpenContents(), ReportFile.Parameters["XML.Xpath"].ToString());

				using (ImportManager)
				{
					ImportManager.BeginImport(this.Delivery);

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

							currencyUnit.Currency.Code = Convert.ToString(reader["Symbol"]);
							currencyUnit.RateDate = Convert.ToDateTime(reader["Date"]);
							currencyUnit.RateValue = Convert.ToDouble(reader["Last"]);
							currencyUnit.DateCreated = DateTime.Today;

							ImportManager.ImportCurrency(currencyUnit);
						}
					}
				}


			}
			return Core.Services.ServiceOutcome.Success;
		}
	}
}
