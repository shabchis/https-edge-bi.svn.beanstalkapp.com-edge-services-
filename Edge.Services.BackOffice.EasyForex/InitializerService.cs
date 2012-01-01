using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Services.SegmentMetrics;
using Edge.Core.Services;

namespace Edge.Services.BackOffice.EasyForex
{
	class InitializerService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			#region Init General
			// ...............................
			// SETUP
			this.Delivery = this.NewDelivery();

			// This is for finding conflicting services
			this.Delivery.Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]", this.Instance.AccountID, this.TargetPeriod.ToAbsolute()));

			// Create an import manager that will handle rollback, if necessary
			SegmentMetricsImportManager importManager = new SegmentMetricsImportManager(this.Instance.InstanceID, new Edge.Data.Pipeline.Common.Importing.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[Edge.Data.Pipeline.Common.Importing.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			// ...............................

			// Now that we have a new delivery, start adding values
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
			};
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = -1
			};

			this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			if (string.IsNullOrEmpty(this.Delivery.TargetLocationDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");

			//TO DO : Add delivery parameters ( URL for file retrieving and etc' ) 

			this.ReportProgress(0.2);
			#endregion

			

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		
		}
	}
}
