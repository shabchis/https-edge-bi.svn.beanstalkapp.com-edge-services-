using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Data.SqlClient;
using Edge.Core.Configuration;

namespace Edge.Services.Metrics.Validations
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
                TargetTable = "";
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
                    },

                    End = new DateTimeSpecification()
                    {
                        BaseDateTime = fromDate,
                        Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Max },
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
        public ValidationResult IsEqual(Dictionary<string, string> Params, Dictionary<string, double> sourceTotals, Dictionary<string, double> targetTotals, string sourceDbName, string targeDbtName)
        {
           
            
            
            if (sourceTotals.Count > 0 && targetTotals.Count > 0)
            {
                bool costAlert = false;
                bool impsAlert = false;
                bool clicksAlert = false;

                double costDif = 0;
                double impsDif = 0;
                double clicksDif = 0;

                if ((costDif = Math.Abs(targetTotals["Cost"] - sourceTotals["Cost"])) > 1) costAlert = true;
                if ((impsDif = Math.Abs(targetTotals["Imps"] - sourceTotals["Imps"])) > 1) impsAlert = true;
                if ((clicksDif = Math.Abs(targetTotals["Clicks"] - sourceTotals["Clicks"])) > 1) clicksAlert = true;


                StringBuilder message = new StringBuilder();
                message.Append(string.Format("Error - Differences has been found for Account ID {0} : ", Params["AccountID"]));
                if (costAlert) message.Append(string.Format(" {0}Cost: {1},{2}Cost: {3}, Diff:{4} ", sourceDbName, sourceTotals["Cost"], targeDbtName, targetTotals["Cost"], costDif));
                if (impsAlert) message.Append(string.Format(" {0}Imps: {1},{2}Imps: {3}, Diff:{4} ", sourceDbName, sourceTotals["Imps"], targeDbtName, targetTotals["Imps"], impsDif));
                if (clicksAlert) message.Append(string.Format(" {0}Clicks: {1},{2}Clicks: {3}, Diff:{4} ", sourceDbName, sourceTotals["Clicks"], targeDbtName, targetTotals["Clicks"], clicksDif));

                if (costAlert || impsAlert || clicksAlert)
                    return new ValidationResult()
                    {
                        ResultType = ValidationResultType.Error,
                        AccountID = Convert.ToInt32(Params["AccountID"]),
                        Message = message.ToString(),
                        TargetPeriodStart = Convert.ToDateTime(Params["Date"]),
                        TargetPeriodEnd = Convert.ToDateTime(Params["Date"]),
                        ChannelID = Convert.ToInt32(Params["ChannelID"]),
                        CheckType = this.Instance.Configuration.Name
                    };


            }
            // Checking if data exists in dwh and not in oltp
            else if (sourceTotals.Count == 0 && targetTotals.Count != 0)
            {
                double SumOfTargetTotals = 0; 
                foreach (var item in targetTotals)
                {
                    SumOfTargetTotals += item.Value;
                }
                if (SumOfTargetTotals !=0)
                return new ValidationResult()
                {
                    ResultType = ValidationResultType.Error,
                    AccountID = Convert.ToInt32(Params["AccountID"]),
                    Message = "Alert- Missing Data in target DB ",
                    ChannelID = Convert.ToInt32(Params["ChannelID"]),
                    TargetPeriodStart = Convert.ToDateTime(Params["Date"]),
                    TargetPeriodEnd = Convert.ToDateTime(Params["Date"]),
                    CheckType = this.Instance.Configuration.Name
                };
            }
            // Checking if data exists in oltp and not in dwh
            else if (targetTotals.Count == 0 && sourceTotals.Count != 0)
            {
                double SumOfsourceTotals = 0;
                foreach (var item in sourceTotals)
                {
                    SumOfsourceTotals += item.Value;
                }
                if (SumOfsourceTotals != 0)
                return new ValidationResult()
                {
                    ResultType = ValidationResultType.Error,
                    AccountID = Convert.ToInt32(Params["AccountID"]),
                    Message = "Alert- Missing Data in Source DB",
                    ChannelID = Convert.ToInt32(Params["ChannelID"]),
                    TargetPeriodStart = Convert.ToDateTime(Params["Date"]),
                    TargetPeriodEnd = Convert.ToDateTime(Params["Date"]),
                    CheckType = this.Instance.Configuration.Name
                };
            }

            return new ValidationResult()
            {
                ResultType = ValidationResultType.Information,
                AccountID = Convert.ToInt32(Params["AccountID"]),
                Message = "Validation Success - no differences",
                ChannelID = Convert.ToInt32(Params["ChannelID"]),
                TargetPeriodStart = Convert.ToDateTime(Params["Date"]),
                TargetPeriodEnd = Convert.ToDateTime(Params["Date"]),
                CheckType = this.Instance.Configuration.Name
            };
        }
        public string GetCubeName(int accountId)
        {
            string cubeName = string.Empty;
            using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString(this, "OltpDB")))
            {
                sqlCon.Open();

//                SqlCommand sqlCommand = new SqlCommand(
//                  @" SELECT SUBSTRING([AccountSettings], CHARINDEX(':', [AccountSettings])+1, CHARINDEX(';',[AccountSettings])-6 )
//                        FROM [dbo].[User_GUI_Account]
//                        where Account_ID = @Account_ID"
//                   );

                SqlCommand sqlCommand = new SqlCommand(
                    @"SELECT AnalysisSettings.value('data(/AnalysisSettings/@CubeName)[1]', 'nvarchar(MAX)')
                        from [dbo].[User_GUI_Account]
                        where Account_ID = @Account_ID"
                    );

                SqlParameter accountIdParam = new SqlParameter("@Account_ID", System.Data.SqlDbType.Int);
                accountIdParam.Value = accountId;
                sqlCommand.Parameters.Add(accountIdParam);

                sqlCommand.Connection = sqlCon;

                using (var _reader = sqlCommand.ExecuteReader())
                {
                    if (!_reader.IsClosed)
                    {
                        while (_reader.Read())
                        {
                            if (!_reader[0].Equals(DBNull.Value))
                            {
                                cubeName = Convert.ToString(_reader[0]);
                            }
                        }
                    }
                }
            }
            return cubeName;
        }
        protected abstract ValidationResult Compare(string SourceTable, string TargetTabel, Dictionary<string, string> Params);

    }
}
