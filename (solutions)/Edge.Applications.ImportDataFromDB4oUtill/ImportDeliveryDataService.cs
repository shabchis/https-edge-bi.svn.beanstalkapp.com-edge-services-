using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Db4objects.Db4o;
using Db4objects.Db4o.Query;
using Edge.Data.Pipeline;
using Db4objects.Db4o.Linq;
using Db4objects.Db4o.Config;

namespace Edge.Applications.ImportDataFromDB4oUtill
{
    class ImportDeliveryDataService : PipelineService
    {
        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            IEmbeddedConfiguration config = Db4oEmbedded.NewConfiguration();
            config.Common.ActivationDepth = 20;


            using (IObjectContainer db = Db4oEmbedded.OpenFile(config, this.Instance.Configuration.Options["DeliveryFilePath"]))
            {
                //IQuery query = db.Query();
                //query.Constrain(typeof(Delivery));

                //DateTime dt = new DateTime(2011, 9, 1);
                //DateTime dt2 = new DateTime(2011, 9, 2);



                //query.Descend("_dateCreated").Constrain(dt).Greater();
                //query.Descend("_dateCreated").Constrain(dt2).Smaller();


                //IObjectSet result = query.Execute();

                var result = from Delivery delivery in db
                             where delivery.DateCreated > DateTime.Now.AddDays(-7)
                             select delivery;
                db.Activate(result, 20);
                
                int error = 0;
                try
                {
                    foreach (var d in result)
                    {

                        if (d.History != null)
                        {
                            int rollbackIndex = -1;
                            int commitIndex = -1;
                            for (int i = 0; i < d.History.Count; i++)
                            {
                                if ((d).History[i].Operation == DeliveryOperation.Committed)
                                    commitIndex = i;
                                else if ((d).History[i].Operation == DeliveryOperation.RolledBack)
                                    rollbackIndex = i;
                            }
                            if (commitIndex > rollbackIndex)
                            {
                                (d).IsCommited = true;
                                d.Save();
                            }


                        }



                    }
                }
                catch (Db4objects.Db4o.Ext.InvalidIDException ex)
                {


                }

                //    for (int j = 0; j < result.Count(); j++)
                //    {
                //        try
                //        {
                //            Delivery d = (Delivery)result[j];
                //            db.Activate(d, 50);
                //            int rollbackIndex = -1;
                //            int commitIndex = -1;
                //            for (int i = 0; i < d.History.Count; i++)
                //            {
                //                if ((d).History[i].Operation == DeliveryOperation.Committed)
                //                    commitIndex = i;
                //                else if ((d).History[i].Operation == DeliveryOperation.RolledBack)
                //                    rollbackIndex = i;
                //            }

                //            if (commitIndex > rollbackIndex)
                //                (d).IsCommited = true;
                //            else (d).IsCommited = false;

                //            if (Delivery.Get((d).DeliveryID) == null)
                //                (d).Save();
                //        }
                //        catch (Exception e)
                //        {
                //            error++;
                //        }

                //    }


            }



            return Core.Services.ServiceOutcome.Success;
        }
    }
}
