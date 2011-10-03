using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;

namespace Edge.Services.AdMetrics.Validations
{
    abstract class DbDbChecksumBaseService : ValidationService
    {
        public double progress = 0;

        protected override IEnumerable<ValidationResult> Validate()
        {
            progress += 0.1;
            this.ReportProgress(progress);

            #region Getting Service option params
            //Getting Accounts list
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["AccountsList"]))
                throw new Exception("Missing Configuration option AccountsList");
            string[] accounts = this.Instance.Configuration.Options["AccountsList"].Split(',');

            //Getting Table 
            string comparisonTable;
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["ComparisonTable"]))
                throw new Exception("Missing Configuration option ComparisonTable");
            else comparisonTable = this.Instance.Configuration.Options["ComparisonTable"];

            //Getting Channel List
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["ChannelList"]))
                throw new Exception("Missing Configuration option ChannelList");
            string[] channels = this.Instance.Configuration.Options["ChannelList"].Split(',');

            //Getting TimePeriod
            DateTime fromDate, toDate;
            if ((String.IsNullOrEmpty(this.Instance.Configuration.Options["fromDate"])) && (String.IsNullOrEmpty(this.Instance.Configuration.Options["toDate"])))
            {
                fromDate = this.TargetPeriod.Start.ToDateTime();
                toDate = this.TargetPeriod.End.ToDateTime();
            }
            else
            {
                fromDate = Convert.ToDateTime(this.Instance.Configuration.Options["fromDate"]);
                toDate = Convert.ToDateTime(this.Instance.Configuration.Options["toDate"]);
            }
            #endregion

            //TO DO : Get connection string and tabels
            yield return Compare();
         
        }
        protected abstract ValidationResult Compare(string SourceDB, string SourceTable, string TargetDB, string TargetTabel);

    }
}
