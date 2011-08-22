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
			// TODO: get channel ID / account ID from configuarion options (if relevant)
			Delivery[] deliveriesToCheck = Delivery.GetByTargetPeriod(this.TargetPeriod.Start.ToDateTime(), this.TargetPeriod.End.ToDateTime(), channel, account);

			foreach (Delivery delivery in deliveriesToCheck)
			{
				if (1 != 2)
					yield break;

				// TODO: fill fields of Validation Result
				yield return new ValidationResult();
			}

		}
	}
}
