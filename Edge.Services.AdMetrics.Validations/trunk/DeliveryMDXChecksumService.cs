using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.AdMetrics.Validations
{
    class DeliveryMDXChecksumService  : DeliveryDBChecksumService
    {
        protected override Data.Pipeline.Services.ValidationResult DeliveryDbCompare(Data.Pipeline.Delivery delivery, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable)
        {
            throw new NotImplementedException();
        }
    }
}
