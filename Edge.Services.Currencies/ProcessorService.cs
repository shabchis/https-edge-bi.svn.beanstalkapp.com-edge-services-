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
			throw new NotImplementedException();

			//USE MAPPING CONFIGURATION FOR THIS SERVICE.

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
						dynamic readerHelper;

						while (ReportReader.Read())
						{
							if (isAttribute)
								readerHelper = ReportReader.Current.Attributes;
							else
								readerHelper = ReportReader.Current;

							CurrencyRate currencyUnit = new CurrencyRate();

						}
					}
				}


			}
		}
	}
}
