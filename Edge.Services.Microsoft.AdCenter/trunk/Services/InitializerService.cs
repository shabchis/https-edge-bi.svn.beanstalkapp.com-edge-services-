using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceModel;
using System.Text;
using Edge.Core;
using Edge.Core.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using WS = Edge.Services.Microsoft.AdCenter.AdCenter.Reporting;
using Edge.Services.AdMetrics;

namespace Edge.Services.Microsoft.AdCenter
{
	public class InitializerService : PipelineService
	{

		protected override ServiceOutcome DoPipelineWork()
		{
			#region Init General
			// ...............................
			// SETUP
			this.Delivery = this.NewDelivery();

			// This is for finding conflicting services
			this.Delivery.Signature =Delivery.CreateSignature( String.Format("Microsoft-AdCenter-[{0}]-[{1}]-[{2}]",
				this.Instance.AccountID,
				this.Instance.Configuration.Options["AdCenter.CustomerID"].ToString(),
				this.TargetPeriod.ToAbsolute()));

			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new AdMetricsImportManager.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[AdMetricsImportManager.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			// ...............................

			// Now that we have a new delivery, start adding values
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				OriginalID = this.Instance.Configuration.Options["AdCenter.CustomerID"].ToString()
			};
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = 14
			};

			this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			if (string.IsNullOrEmpty(this.Delivery.TargetLocationDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");
			// Copy some options as delivery parameters
			
			
			this.ReportProgress(0.2);
			#endregion

			// Wrapper for adCenter API
			AdCenterApi adCenterApi = new AdCenterApi(this);

			// ................................
			// Campaign report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.CampaignReport });

			ReportProgress(0.33);

			// ................................
			// Ad report


			ReportProgress(0.33);

			// ................................
			// Keyword report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.KeywordReport });



			this.Delivery.Files[Const.Files.KeywordReport].Parameters.Add(Const.Parameters.TimePeriodColumnName, AdCenterApi.GetTimePeriodColumnName(WS.ReportAggregation.Daily));


			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.AdReport });
			ReportProgress(0.33);

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

	}
}
