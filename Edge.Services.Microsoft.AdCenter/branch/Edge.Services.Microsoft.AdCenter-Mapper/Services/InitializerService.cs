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
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Common.Importing;

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
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				OriginalID = this.Instance.Configuration.Options["AdCenter.CustomerID"].ToString()
			};
			this.Delivery.TimePeriodDefinition = this.TimePeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = 14
			};

			this.Delivery.FileDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			if (string.IsNullOrEmpty(this.Delivery.FileDirectory))
				throw new Exception("Delivery FileDirectory must be configured in configuration file (DeliveryFilesDir)");
			// Copy some options as delivery parameters
			

			this.Delivery.Outputs.Add(new DeliveryOutput()
			{
				Signature = Delivery.CreateSignature( String.Format("Microsoft-AdCenter-[{0}]-[{1}]-[{2}]",
				this.Instance.AccountID,
				this.Instance.Configuration.Options["AdCenter.CustomerID"].ToString(),
				this.TimePeriod.ToAbsolute()
				)),

				Account = new Data.Objects.Account() { ID = this.Instance.AccountID, OriginalID = this.Instance.Configuration.Options["AdCenter.CustomerID"] },
				Channel = new Data.Objects.Channel() { ID = 14 },
				TimePeriodStart = Delivery.TimePeriodStart,
				TimePeriodEnd = Delivery.TimePeriodEnd
			}
			);

			// Create an import manager that will handle rollback, if necessary
			AdMetricsImportManager importManager = new AdMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[Edge.Data.Pipeline.Metrics.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			// ...............................

			// Now that we have a new delivery, start adding values
			
			this.ReportProgress(0.2);
			#endregion

			// Wrapper for adCenter API
			AdCenterApi adCenterApi = new AdCenterApi(this);

			// ................................
			// Campaign report
			this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.CampaignReport});

			ReportProgress(0.33);

			// ................................
			// Ad report


			ReportProgress(0.33);

			// ................................
			// Keyword report
            this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.KeywordReport});



			this.Delivery.Files[Const.Files.KeywordReport].Parameters.Add(Const.Parameters.TimePeriodColumnName, AdCenterApi.GetTimePeriodColumnName(WS.ReportAggregation.Daily));


            this.Delivery.Files.Add(new DeliveryFile() { Name = Const.Files.AdReport});
			ReportProgress(0.33);

			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

    }
}
