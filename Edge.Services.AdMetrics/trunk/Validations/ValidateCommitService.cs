using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.AdMetrics
{
	public class ValidateCommitService : ValidationService
	{
		protected override ValidationResult Validate()
		{
			DeliveryHistoryEntry importEntry = this.Delivery.History.Last(entry => entry.Operation == DeliveryOperation.Imported);
			DeliveryHistoryEntry commitEntry = this.Delivery.History.Last(entry => entry.Operation == DeliveryOperation.Committed);
			DeliveryHistoryEntry rollbackEntry = this.Delivery.History.Last(entry => entry.Operation == DeliveryOperation.RolledBack);

			// Was never imported
			if (importEntry == null)
				throw new InvalidOperationException("The delivery has not been imported yet.");

			// Was not committed or was imported again
			if (commitEntry == null || this.Delivery.History.IndexOf(commitEntry) < this.Delivery.History.IndexOf(importEntry))
				throw new InvalidOperationException("The delivery has not been committed yet.");

			// Was rolled back
			if (rollbackEntry != null && this.Delivery.History.IndexOf(rollbackEntry) > this.Delivery.History.IndexOf(commitEntry))
				throw new InvalidOperationException("The delivery has been rolled back.");

			//Dictionary<string,Measure> measures = Measure.GetMeasures(this.Delivery.Account, this.Delivery.Channel, null, MeasureOptions.IsCalculated | MeasureOptions.IsTarget, MeasureOptionsOperator.Not);
			//foreach(Measure measure in m

			return new ValidationResult()
			{
				Success = true
				// Parameters = new Dictionary<string,object>() { ... }
			};
		}
	}
}
