using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Services.Common.Validation;


namespace Edge.Services.AdMetrics.Validations
{
    class DeliveryMdxChecksumService : DeliveryDBChecksumBaseService
    {
        protected override ValidationResult DeliveryDbCompare(Data.Pipeline.Delivery delivery, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable)
        {
            throw new NotImplementedException();
        }
    }
}
