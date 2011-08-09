using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.AdMetrics.Validations
{
	public class DeliveryOltpChecksumService: ValidationService
	{
		protected override IEnumerable<ValidationResult> Validate()
		{
			Delivery d = Delivery.GetByTargetPeriod(channelID, accountID, start, end);


			Delivery[] deliveriesToCheck;

			foreach (Delivery delivery in deliveriesToCheck)
			{
				yield return new ValidationResult()
				{

				};
			}
		}
	}
}
