using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;

namespace Edge.Services.Facebook.AdsApi
{
	public class InitializerService: PipelineService
	{
		protected override ServiceOutcome DoWork()
		{
			// Create a new delivery
			this.Delivery = new Delivery(this.Instance.InstanceID)
			{
				TargetPeriod = this.TargetPeriod
			};

			// TODO: add delivery files
			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = "myFile",
				ReaderType = typeof(XPathRowReader<PpcDataUnit>)
			});


			this.Delivery.Save();

			return ServiceOutcome.Success;
		}
	}
}
