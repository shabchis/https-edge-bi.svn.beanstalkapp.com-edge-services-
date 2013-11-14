//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using Edge.Core.Services;
//using Edge.Data.Pipeline;
//using Edge.Data.Pipeline.Services;
//using Edge.Core.Utilities;
//using Edge.Data.Pipeline.Metrics;

//namespace Edge.Services.Currencies
//{
//    public class MetricsCommitService : PipelineService
//    {
//        protected override ServiceOutcome DoPipelineWork()
//        {
//            // ----------------
//            // SETUP

//            string checksumThreshold = Instance.Configuration.Options[Consts.ConfigurationOptions.ChecksumTheshold];

//            MetricsImportManagerOptions options = new MetricsImportManagerOptions()
//            {
//                SqlTransformCommand = Instance.Configuration.Options[Consts.AppSettings.SqlTransformCommand],
//                SqlStageCommand = Instance.Configuration.Options[Consts.AppSettings.SqlStageCommand],
//                SqlRollbackCommand = Instance.Configuration.Options[Consts.AppSettings.SqlRollbackCommand],
//                //ChecksumThreshold = checksumThreshold == null ? 0.01 : double.Parse(checksumThreshold)
//            };

//            string importManagerTypeName = Instance.Configuration.GetOption(Consts.ConfigurationOptions.ImportManagerType);
//            Type importManagerType = Type.GetType(importManagerTypeName);

//            var importManager = (CurrencyImportManager) Activator.CreateInstance(importManagerType, this.Instance.InstanceID,options);
//            ReportProgress(0.1);

//            // ----------------
//            // TICKETS

//            // Only check tickets, don't check conflicts
//            this.HandleConflicts(importManager, DeliveryConflictBehavior.Ignore, getBehaviorFromConfiguration: false);
//            ReportProgress(0.6);

//            // ----------------
//            // COMMIT
//            bool success = false;
//            do
//            {
//                try
//                {
//                    Log.Write("Commit: start", LogMessageType.Information);
//                    importManager.Commit(new Delivery[] { this.Delivery });
//                    Log.Write("Commit: end", LogMessageType.Information);
//                    success = true;
//                }
//                catch (DeliveryConflictException dceex)
//                {
//                    Log.Write("Rollback: start", LogMessageType.Information);
//                    importManager.RollbackOutputs(dceex.ConflictingOutputs);
//                    Log.Write("Rollback: end", LogMessageType.Information);
//                }
//                catch (Exception ex)
//                {
//                    throw new Exception(String.Format("Delivery {0} failed during staging.", this.Delivery.DeliveryID), ex);
//                }
//            }
//            while (!success);

//            return ServiceOutcome.Success;
//        }
//    }
//}
