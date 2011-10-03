using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;

namespace Edge.Services.AdMetrics.Validations
{
    abstract class DbDbChecksumBaseService : ValidationService
    {
        public double progress = 0;
        Dictionary<string, string> Params = new Dictionary<string, string>();

        protected override IEnumerable<ValidationResult> Validate()
        {
            progress += 0.1;
            this.ReportProgress(progress);

            #region Getting Service option params
            //Getting Accounts list
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["AccountsList"]))
                throw new Exception("Missing Configuration option AccountsList");
            string[] accounts = this.Instance.Configuration.Options["AccountsList"].Split(',');

            //Getting Tables 
            string SourceTable;
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["SourceTable"]))
                throw new Exception("Missing Configuration option SourceTable");
            else SourceTable = this.Instance.Configuration.Options["SourceTable"];

            string TargetTable;
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["TargetTable"]))
                throw new Exception("Missing Configuration option TargetTable");
            else TargetTable = this.Instance.Configuration.Options["TargetTable"];

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

            while (fromDate <= toDate)
            {
                // {start: {base : '2009-01-01', h:0}, end: {base: '2009-01-01', h:'*'}}
                var subRange = new DateTimeRange()
                {
                    Start = new DateTimeSpecification()
                    {
                        BaseDateTime = fromDate,
                        Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Exact, Value = 0 },
                        Boundary = DateTimeSpecificationBounds.Lower
                    },

                    End = new DateTimeSpecification()
                    {
                        BaseDateTime = fromDate,
                        Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Max },
                        Boundary = DateTimeSpecificationBounds.Upper
                    }
                };

                foreach (string account in accounts)
                {
                    foreach (string channel in channels)
                    {
                        yield return Compare(SourceTable, TargetTable, new Dictionary<string, string>() 
                        {
                           {"AccountID",account},
                           {"ChannelID",channel},
                           {"Date",fromDate.ToString()}
                        });
                    }

                }
                fromDate = fromDate.AddDays(1);
            }

        }
        protected abstract ValidationResult Compare(string SourceTable, string TargetTabel, Dictionary<string, string> Params);

    }
}
