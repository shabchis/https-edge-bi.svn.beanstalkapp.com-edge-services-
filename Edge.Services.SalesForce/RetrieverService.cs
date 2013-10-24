using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Data;
using System.Data.SqlClient;
using Edge.Data.Pipeline;
using Edge.Core.Data;
using Edge.Core.Configuration;
using System.Net;
using System.Threading;
using Newtonsoft.Json;
using System.IO;
using System.Security.Policy;
using Newtonsoft.Json.Converters;
using System.Text.RegularExpressions;


namespace Edge.Services.SalesForce
{
    class RetrieverService : PipelineService
    {
        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            Mutex mutex = new Mutex(false, "SalesForceRetriver");
            BatchDownloadOperation batch = new BatchDownloadOperation();


            try
            {
                mutex.WaitOne();
                #region Authentication
                //get access token + refresh token from db (if exist)
                Token tokenResponse = Token.Get(Delivery.Parameters["SalesForceClientID"].ToString(), this.Delivery.Account.ID);
                //if not exist
                if (string.IsNullOrEmpty(tokenResponse.access_token) || (string.IsNullOrEmpty(tokenResponse.refresh_token)))
                    tokenResponse = GetAccessTokenParamsFromSalesForce();


                //check if access_token is not expired
                if (tokenResponse.UpdateTime.Add((TimeSpan.Parse(AppSettings.Get(tokenResponse, "TimeOut")))) < DateTime.Now)
                    tokenResponse = RefreshToken(tokenResponse.refresh_token);


                #endregion
                // exist
                foreach (var file in Delivery.Files)
                {


                    string query = file.Parameters["Query"].ToString();

                    //Regex for Calendar units.
                    MatchCollection calendarMatches = Regex.Matches(query, @"TimePeriod.EqualToCalendarUnits\(([A-Z\.a-z_]+)\)", RegexOptions.IgnoreCase);
                    if (calendarMatches.Count > 0)
                    {
                        foreach (Match calendarMatch in calendarMatches)
                        {
                            string dataParamName = calendarMatch.Groups[1].Value;
                            query = query.Replace(string.Format("TimePeriod.EqualToCalendarUnits({0})", dataParamName), string.Format(" CALENDAR_YEAR({0})={1} AND CALENDAR_MONTH({0})={2} AND DAY_IN_MONTH({0}) = {3} ", dataParamName, Delivery.TimePeriodStart.Year, Delivery.TimePeriodStart.Month, Delivery.TimePeriodStart.Day));
                        }
                    }

                    //Regex for TimePeriodStringFormat units.
                    MatchCollection timeMatches = Regex.Matches(query, @"TimePeriod.EqualToString\(([A-Z\.a-z_]+)\)", RegexOptions.IgnoreCase);
                    if (timeMatches.Count > 0)
                    {
                        foreach (Match calendarMatch in timeMatches)
                        {
                            string dataParamName = calendarMatch.Groups[1].Value;
                            string sfTimePeriodStartFormat = string.Format("{0}T00:00:00.00Z", Delivery.TimePeriodStart.ToString("yyyy-MM-dd"));
                            string sfTimePeriodEndFormat = string.Format("{0}T23:59:59.59Z", Delivery.TimePeriodStart.ToString("yyyy-MM-dd"));
                            query = query.Replace(string.Format("TimePeriod.EqualToString({0})", dataParamName), string.Format("{0}>{1} AND {0}<{2} ", dataParamName, sfTimePeriodStartFormat, sfTimePeriodEndFormat));
                        }
                    }
                    file.Parameters.Add("Token", tokenResponse);
                    file.SourceUrl = string.Format("{0}/services/data/v20.0/query?q={1}", tokenResponse.instance_url, query);

                    HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);
                    request.Headers.Add("Authorization: OAuth " + tokenResponse.access_token);

                    //check if response contains more than one file 

                    FileDownloadOperation fileDownloadOperation = file.Download(request);
                    batch.Add(fileDownloadOperation);
                }
                batch.Start();
                batch.Wait();
                batch.EnsureSuccess();

                //supporting more than one file per query
                int offset = 1;
               // FetchNext(this.Delivery.Files, offset);
              
            }
            finally
            {
                mutex.ReleaseMutex();
            }


            Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }

        private List<DeliveryFile> FetchNext(List<DeliveryFile> fetchFrom, int offset)
        {
            BatchDownloadOperation nextBatch = new BatchDownloadOperation();
            List<DeliveryFile> nextRecordsFiles = new List<DeliveryFile>();
            foreach (DeliveryFile ReportFile in fetchFrom)
            {
                //setting cuurent file has batched and batching next file
                ReportFile.Parameters.Add("Batch", true);


                string fileName = ReportFile.Name + "-" + offset;

                JsonDynamicReader reportReader = new JsonDynamicReader(ReportFile.OpenContents(compression: FileCompression.None), "$.nextRecordsUrl");
                string nextRecordPath;
                if (reportReader.Read())
                {
                    nextRecordPath = reportReader.Current.nextRecordsUrl;
                    DeliveryFile nextRecordFile = new DeliveryFile();
                    nextRecordFile.SourceUrl = ((Token)(ReportFile.Parameters["Token"])).instance_url + nextRecordPath;

                    HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(nextRecordFile.SourceUrl);
                    request.Headers.Add("Authorization: OAuth " + ((Token)(ReportFile.Parameters["Token"])).access_token);

                    //check if response contains more than one file 

                    FileDownloadOperation fileDownloadOperation = nextRecordFile.Download(request);
                    nextBatch.Add(fileDownloadOperation);

                    nextRecordsFiles.Add(nextRecordFile);
                }
            }
            if (nextRecordsFiles.Count > 0)
            {
                nextBatch.Start();
                nextBatch.Wait();
                nextBatch.EnsureSuccess();

                foreach (DeliveryFile file in FetchNext(nextRecordsFiles, offset))
                {
                    this.Delivery.Files.Add(file);
                }
            }

            return nextRecordsFiles;
        }

        public Token RefreshToken(string refreshToken)
        {
            HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(Delivery.Parameters["AuthenticationUrl"].ToString());
            myRequest.Method = "POST";
            myRequest.ContentType = "application/x-www-form-urlencoded";

            using (StreamWriter writer = new StreamWriter(myRequest.GetRequestStream()))
            {
                writer.Write(string.Format("refresh_token={0}&client_id={1}&client_secret={2}&grant_type=refresh_token",
                    refreshToken,
                    Delivery.Parameters["SalesForceClientID"],
                    Delivery.Parameters["ClientSecret"]));
            }

            HttpWebResponse myResponse = (HttpWebResponse)myRequest.GetResponse();
            Stream responseBody = myResponse.GetResponseStream();

            Encoding encode = System.Text.Encoding.GetEncoding("utf-8");


            StreamReader readStream = new StreamReader(responseBody, encode);


            Token tokenResponse;
            tokenResponse = (Token)JsonConvert.DeserializeObject(readStream.ReadToEnd(), typeof(Token));
            tokenResponse.refresh_token = refreshToken;
            tokenResponse.UpdateTime = DateTime.Now;
            tokenResponse.Save(Delivery.Parameters["SalesForceClientID"].ToString(), this.Delivery.Account.ID);
            return tokenResponse;
        }

        public Token GetAccessTokenParamsFromSalesForce()
        {
            HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(Delivery.Parameters["AuthenticationUrl"].ToString());
            myRequest.Method = "POST";
            myRequest.ContentType = "application/x-www-form-urlencoded";


            using (StreamWriter writer = new StreamWriter(myRequest.GetRequestStream()))
            {
                writer.Write(string.Format("code={0}&grant_type=authorization_code&client_id={1}&client_secret={2}&redirect_uri={3}",
                    Delivery.Parameters["ConsentCode"],
                    Delivery.Parameters["SalesForceClientID"],
                    Delivery.Parameters["ClientSecret"],
                    Delivery.Parameters["Redirect_URI"]));
            }
            HttpWebResponse myResponse;
            try
            {
                myResponse = (HttpWebResponse)myRequest.GetResponse();

            }
            catch (WebException webEx)
            {
                using (StreamReader reader = new StreamReader(webEx.Response.GetResponseStream()))
                {
                    throw new Exception(reader.ReadToEnd());

                }


            }

            Stream responseBody = myResponse.GetResponseStream();
            Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
            StreamReader readStream = new StreamReader(responseBody, encode);
            Token token = JsonConvert.DeserializeObject<Token>(readStream.ReadToEnd());
            token.UpdateTime = DateTime.Now;
            token.Save(this.Delivery.Parameters["SalesForceClientID"].ToString(), this.Delivery.Account.ID);  //SalesForceClientID is the Consumer Key
            //return string itself (easier to work with)
            return token;
        }







        public EventHandler batch_Progressed { get; set; }
    }
    public class Token
    {
        public string id { get; set; }
        public string issued_at { get; set; }
        public DateTime UpdateTime { get; set; }
        public string refresh_token { get; set; }
        public string instance_url { get; set; }
        public string signature { get; set; }
        public string access_token { get; set; }
        public string ClientID { get; set; }

        internal void Save(string clientID, int accountId)
        {

            Token tokenResponse = new Token();

            using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(tokenResponse, "DB")))
            {
                using (SqlCommand command = DataManager.CreateCommand(AppSettings.Get(typeof(Token), "SP_Save"), CommandType.StoredProcedure))
                {
                    conn.Open();
                    command.Connection = conn;
                    command.Parameters["@AccountID"].Value = accountId;
                    command.Parameters["@Id"].Value = this.id;
                    command.Parameters["@ClientID"].Value = clientID;
                    command.Parameters["@Instance_url"].Value = this.instance_url;
                    command.Parameters["@AccessToken"].Value = this.access_token;
                    command.Parameters["@RefreshToken"].Value = this.refresh_token;
                    command.Parameters["@Signature"].Value = this.signature;
                    command.Parameters["@Issued_at"].Value = this.issued_at;
                    command.Parameters["@UpdateTime"].Value = DateTime.Now;

                    command.ExecuteNonQuery();

                }
            }

        }

        public static Token Get(string clientID, int accountId)
        {
            Token tokenResponse = new Token();
            using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(tokenResponse, "DB")))
            {
                using (SqlCommand command = DataManager.CreateCommand(AppSettings.Get(typeof(Token), "SP_Get"), CommandType.StoredProcedure))
                {
                    conn.Open();
                    command.Connection = conn;
                    command.Parameters["@ClientID"].Value = clientID;
                    command.Parameters["@AccountID"].Value = accountId;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();
                            tokenResponse.UpdateTime = Convert.ToDateTime(reader["UpdateTime"]);
                            tokenResponse.ClientID = clientID;
                            tokenResponse.id = reader["Id"].ToString();
                            tokenResponse.access_token = reader["AccessToken"].ToString();
                            tokenResponse.issued_at = reader["Issued_at"].ToString();
                            tokenResponse.instance_url = reader["Instance_url"].ToString();
                            tokenResponse.signature = reader["Signature"].ToString();
                            tokenResponse.refresh_token = reader["RefreshToken"].ToString();
                        }
                    }
                }
            }
            return tokenResponse;
        }
    }

}
