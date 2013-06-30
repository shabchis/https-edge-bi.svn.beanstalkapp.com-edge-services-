using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using GA201302 = Google.Api.Ads.AdWords.v201302;
using Google.Api.Ads.AdWords.Util;
using System.Threading;
using Edge.Core.Utilities;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util.Reports;
using System.IO;
using Edge.Core.Configuration;
using System.Web;
using System.Xml.Serialization;
using Google.Api.Ads.Common.Lib;
using Google.Api.Ads.Common.Util;

namespace Edge.Services.Google.AdWords
{
    class RetrieverService : PipelineService
    {
        private const int MAX_ERROR_LENGTH = 4096;
        private const string REPORT_ERROR_REGEX = "\\!\\!\\!([^\\|]*)\\|\\|\\|(.*)";

        #region members
        private BatchDownloadOperation _batchDownloadOperation;
        private int _filesInProgress = 0;
        private double _minProgress = 0.05;
        GA201302.ReportDefinitionDateRangeType _dateRange;
        private AutoResetEvent _waitHandle;
        long _reportId;

        #endregion

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {

            _batchDownloadOperation = new BatchDownloadOperation() { MaxConcurrent = 1 };
            _batchDownloadOperation.Progressed += new EventHandler(_batchDownloadOperation_Progressed);
            _filesInProgress = this.Delivery.Files.Count;
            bool includeZeroImpression = Boolean.Parse(this.Delivery.Parameters["includeZeroImpression"].ToString());


            //Sets Date Range and time period
            _dateRange = GA201302.ReportDefinitionDateRangeType.CUSTOM_DATE;
            string startDate = this.TimePeriod.Start.ToDateTime().ToString("yyyyMMdd");
            string endDate = this.TimePeriod.End.ToDateTime().ToString("yyyyMMdd");
            _waitHandle = new AutoResetEvent(false);

            foreach (string clientId in (string[])this.Delivery.Parameters["AdwordsClientIDs"])
            {
                //Get all files on specific client
                var files = from f in this.Delivery.Files
                            where f.Parameters["AdwordsClientID"].ToString() == clientId
                            select f;

                //Setting Adwords User
                Dictionary<string, string> headers = new Dictionary<string, string>()
						{
							{"DeveloperToken" ,this.Delivery.Parameters["DeveloperToken"].ToString()},
							{"UserAgent" , FileManager.UserAgentString},
							{"EnableGzipCompression","true"},
							{"ClientCustomerId",clientId},
							{"Email",this.Delivery.Parameters["MccEmail"].ToString()}
							//{"Password",this.Delivery.Parameters["MccPass"].ToString()}
						};

                AdWordsUser user = new AdWordsUser(headers);
                bool firstCheck = true;
                List<string> awqls = new List<string>();

                foreach (DeliveryFile file in files)
                {
                    GA201302.ReportDefinitionReportType reportType;

                    //Get Google report type 
                    if (Enum.IsDefined(typeof(GA201302.ReportDefinitionReportType), file.Parameters["ReportType"].ToString()))
                        reportType = (GA201302.ReportDefinitionReportType)Enum.Parse(typeof(GA201302.ReportDefinitionReportType), file.Parameters["ReportType"].ToString(), true);
                    else
                        throw new Exception("Google Adwords Report Type Error ! Could not find Enum value for report type");

                    if (Enum.IsDefined(typeof(GA201302.ReportDefinitionReportType), file.Parameters["ReportType"].ToString()))
                        reportType = (GA201302.ReportDefinitionReportType)Enum.Parse(typeof(GA201302.ReportDefinitionReportType), file.Parameters["ReportType"].ToString(), true);
                    else
                        throw new Exception("Google Adwords Report Type Error ! Could not find Enum value for report type");



                    //Getting AuthToken
                    (user.Config as AdWordsAppConfig).AuthToken = AdwordsUtill.GetAuthToken(user);

                    //Creating AWQL
                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT ");
                    foreach (string item in GoogleStaticReportFields.REPORTS_FIELDS[reportType][file.Parameters["ReportFieldsType"].ToString()])
                    {
                        sb.Append(item);
                        sb.Append(",");
                    }
                    sb.Remove(sb.Length - 1, 1); // removing last ","
                    sb.Append(" FROM " + reportType.ToString());

                    if (!includeZeroImpression && GoogleStaticReportFields.REPORTS_FIELDS[reportType][file.Parameters["ReportFieldsType"].ToString()].Contains(""))
                        sb.Append(" WHERE Impressions > 0");


                    if (file.Name.Equals(GoogleStaticReportsNamesUtill._reportNames[GA201302.ReportDefinitionReportType.PLACEHOLDER_FEED_ITEM_REPORT]))
                    //Site link file
                    {
                        if (sb.ToString().Contains("WHERE"))
                            sb.Append("AND ClickType IN [SITELINKS]");
                        else sb.Append("WHERE ClickType IN [SITELINKS] ");
                    }

                    sb.Append(string.Format(" DURING {0},{1}", startDate, endDate));

                    AdWordsAppConfig config = (AdWordsAppConfig)user.Config;

                    string QUERY_REPORT_URL_FORMAT = "{0}/api/adwords/reportdownload/{1}?" + "__fmt={2}";
                    string reportVersion = "v201302";
                    string format = GA201302.DownloadFormat.GZIPPED_CSV.ToString();
                    file.SourceUrl = string.Format(QUERY_REPORT_URL_FORMAT, config.AdWordsApiServer, reportVersion, format);
                    string query = sb.ToString();
                    string postData = string.Format("__rdquery={0}", HttpUtility.UrlEncode(query));

                    awqls.Add(query);

                    //Validate Report
                    if (firstCheck)
                    {
                        string error = string.Empty;
                        if (!ValidateReport(file, user, postData, out error))
                        {
                            //CHEKING FOR INVALID AUTHTOKEN
                            if (error.Contains(GA201302.AuthenticationErrorReason.GOOGLE_ACCOUNT_COOKIE_INVALID.ToString()))
                            {
                                //RENEWING AUTHTOKEN
                                (user.Config as AdWordsAppConfig).AuthToken = AdwordsUtill.GetAuthToken(user, generateNew: true);
                            }
                            else throw new Exception("Google Adwords API Error: " + error);
                        }
                        firstCheck = !firstCheck;
                    }

                    //If Validate - Success
                    DownloadFile(file, user, postData);

                }
            }

            _batchDownloadOperation.Start();
            _batchDownloadOperation.Wait();

            _batchDownloadOperation.EnsureSuccess(); //INCASE OF GENERAL EXCEPTION OPEN DELIVERY FILE HAS HTML AND VIEW INNER ERROR

            this.Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }

        private void DownloadFile(DeliveryFile file, AdWordsUser user, string postData)
        {

            WebRequest request = CreateAdwordsReportRequest(file, user, postData);
            FileDownloadOperation operation = file.Download(request);
            operation.RequestBody = postData;

            _batchDownloadOperation.Add(operation);

        }
        private WebRequest CreateAdwordsReportRequest(DeliveryFile file, AdWordsUser user, string postBody)
        {
            WebRequest request = HttpWebRequest.Create(file.SourceUrl);
            request.Timeout = 100000;

            if (!string.IsNullOrEmpty(postBody))
            {
                request.Method = "POST";
            }
            if (!string.IsNullOrEmpty((user.Config as AdWordsAppConfig).ClientCustomerId))
            {
                request.Headers.Add("clientCustomerId: " + (user.Config as AdWordsAppConfig).ClientCustomerId);
            }

            request.ContentType = "application/x-www-form-urlencoded";

            if ((user.Config as AdWordsAppConfig).EnableGzipCompression)
            {
                (request as HttpWebRequest).AutomaticDecompression = DecompressionMethods.GZip
                    | DecompressionMethods.Deflate;
            }
            else
            {
                (request as HttpWebRequest).AutomaticDecompression = DecompressionMethods.None;
            }

            if ((user.Config as AdWordsAppConfig).AuthorizationMethod == AdWordsAuthorizationMethod.ClientLogin)
            {
                string authToken = AdwordsUtill.GetAuthToken(user);
                (user.Config as AdWordsAppConfig).AuthToken = authToken;
                request.Headers["Authorization"] = "GoogleLogin auth=" + authToken;
            }

            request.Headers.Add("returnMoneyInMicros: true");
            request.Headers.Add("developerToken: " + (user.Config as AdWordsAppConfig).DeveloperToken);

            //Try to unmark the following comment in case of api error 
            //The client library will use only apiMode = true.
            //request.Headers.Add("apiMode", "true");

            return request;
        }
        void _batchDownloadOperation_Progressed(object sender, EventArgs e)
        {
            //BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
            //this.ReportProgress(DownloadOperation.Progress);
        }

        private bool DownloadReportToStream(string downloadUrl, AdWordsAppConfig config, bool returnMoneyInMicros, Stream outputStream, string postBody, AdWordsUser user)
        {
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(downloadUrl);
            if (!string.IsNullOrEmpty(postBody))
            {
                request.Method = "POST";
            }
            request.Proxy = config.Proxy;
            request.Timeout = config.Timeout;
            request.UserAgent = config.GetUserAgent();

            if (!string.IsNullOrEmpty(config.ClientEmail))
            {
                request.Headers.Add("clientEmail: " + config.ClientEmail);
            }
            else if (!string.IsNullOrEmpty(config.ClientCustomerId))
            {
                request.Headers.Add("clientCustomerId: " + config.ClientCustomerId);
            }
            request.ContentType = "application/x-www-form-urlencoded";
            if (config.EnableGzipCompression)
            {
                (request as HttpWebRequest).AutomaticDecompression = DecompressionMethods.GZip
                    | DecompressionMethods.Deflate;
            }
            else
            {
                (request as HttpWebRequest).AutomaticDecompression = DecompressionMethods.None;
            }
            if (config.AuthorizationMethod == AdWordsAuthorizationMethod.OAuth2)
            {
                if (user.OAuthProvider != null)
                {
                    request.Headers["Authorization"] = user.OAuthProvider.GetAuthHeader(downloadUrl);
                }
                else
                {
                    //throw new AdWordsApiException(null, AdWordsErrorMessages.OAuthProviderCannotBeNull);
                }
            }
            else if (config.AuthorizationMethod == AdWordsAuthorizationMethod.ClientLogin)
            {
                string authToken = (!string.IsNullOrEmpty(config.AuthToken)) ? config.AuthToken :
                    new AuthToken(config, AdWordsSoapClient.SERVICE_NAME, config.Email,
                        config.Password).GetToken();
                request.Headers["Authorization"] = "GoogleLogin auth=" + authToken;
            }

            request.Headers.Add("returnMoneyInMicros: " + returnMoneyInMicros.ToString().ToLower());
            request.Headers.Add("developerToken: " + config.DeveloperToken);
            // The client library will use only apiMode = true.
            request.Headers.Add("apiMode", "true");

            if (!string.IsNullOrEmpty(postBody))
            {
                using (StreamWriter writer = new StreamWriter(request.GetRequestStream()))
                {
                    writer.Write(postBody);
                }
            }

            // AdWords API now returns a 400 for an API error.
            bool retval = false;
            WebResponse response = null;
            try
            {
                response = request.GetResponse();
                retval = true;
            }
            catch (WebException ex)
            {
                response = ex.Response;
                byte[] preview = ConvertStreamToByteArray(response.GetResponseStream(), MAX_ERROR_LENGTH);
                string previewString = ConvertPreviewBytesToString(preview);
                retval = false;
            }
            MediaUtilities.CopyStream(response.GetResponseStream(), outputStream);
            response.Close();
            return retval;
        }
        private bool ValidateReport(DeliveryFile file, AdWordsUser user, string postData, out string ErrorMsg)
        {
            ErrorMsg = string.Empty;
            WebRequest request = CreateAdwordsReportRequest(file, user, postData);
            request.Proxy = null;
            request.Headers.Add("validateOnly: true"); // Add Validate

            using (StreamWriter writer = new StreamWriter(request.GetRequestStream()))
            {
                writer.Write(postData);
            }

            //Try to ger response from server
            WebResponse response = null;
            try
            {
                response = request.GetResponse();
            }
            catch (WebException we)
            {
                response = we.Response;
                byte[] preview = ConvertStreamToByteArray(response.GetResponseStream(), MAX_ERROR_LENGTH);
                string previewString = ConvertPreviewBytesToString(preview);

                if (!string.IsNullOrEmpty(previewString))
                {
                    //BUG fix due to adwords new version changes
                    //if (Regex.IsMatch(previewString, REPORT_ERROR_REGEX))
                    if ((previewString.Contains(typeof(GA201302.AuthorizationError).Name)) || (previewString.Contains(typeof(GA201302.AuthenticationError).Name)))
                    {
                        ErrorMsg = previewString;
                        return false;
                    }
                    else throw new Exception(previewString);
                }
            }

            return true;
        }
        private string ConvertPreviewBytesToString(byte[] previewBytes)
        {
            if (previewBytes == null)
            {
                return "";
            }

            // It is possible that our byte array doesn't end at a valid utf-8 string
            // boundary, so we use a progressive decoder to decode bytes as far as
            // possible.
            Decoder decoder = Encoding.UTF8.GetDecoder();
            char[] charArray = new char[previewBytes.Length];
            int bytesUsed;
            int charsUsed;
            bool completed;

            decoder.Convert(previewBytes, 0, previewBytes.Length, charArray, 0, charArray.Length, true,
                out bytesUsed, out charsUsed, out completed);
            return new string(charArray, 0, charsUsed);
        }
        private static byte[] ConvertStreamToByteArray(Stream sourceStream, int maxPreviewBytes)
        {
            if (sourceStream == null)
            {
                throw new ArgumentNullException("sourceStream");
            }

            int bufferSize = 2 << 20;
            byte[] buffer = new byte[bufferSize];
            List<Byte> byteArray = new List<byte>();

            int bytesRead = 0;
            while ((bytesRead = sourceStream.Read(buffer, 0, bufferSize)) != 0)
            {
                int index = 0;
                while (byteArray.Count < maxPreviewBytes && index < bytesRead)
                {
                    byteArray.Add(buffer[index]);
                    index++;
                }
            }
            return byteArray.ToArray();
        }
    }
}
