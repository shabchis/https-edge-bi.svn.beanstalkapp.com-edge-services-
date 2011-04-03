using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Core.Services;

namespace Edge.Services.Microsoft.AdCenter
{
	public class AdCenterInitializerService : AdCenterInitializerBase
	{
		protected override ServiceOutcome DoWork()
		{
			// Create a new delivery
			this.Delivery = new Delivery(this.Instance.InstanceID)
			{
				TargetPeriod = this.TargetPeriod
			};

			// AccountID as parameter for entire delivery
			this.Delivery.Parameters["AccountID"] = this.Instance.AccountID;

			// Both keyword and ad performance reports are needed
			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = "KeywordPerformanceReport",
				SourceUrl = SubmitReportRequest(NewKeywordPerformanceReportRequest()),
				ReaderType = typeof(KeywordPerformanceReportReader)
			});
			ReportProgress(0.49); // progress: 49%

			this.Delivery.Files.Add(new DeliveryFile()
			{
				Name = "AdPerformanceReport",
				SourceUrl = SubmitReportRequest(NewAdPerformanceReportRequest()),
				ReaderType = typeof(AdPerformanceReportReader)
			});
			ReportProgress(0.98); // progress: 98%

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}
	}
}
