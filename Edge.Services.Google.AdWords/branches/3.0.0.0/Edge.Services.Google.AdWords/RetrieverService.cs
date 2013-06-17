using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using GA201302 = Google.Api.Ads.AdWords.v201302;
using Google.Api.Ads.AdWords.Lib;
using System.IO;
using System.Web;
using Google.Api.Ads.Common.Lib;
using Google.Api.Ads.Common.Util;

namespace Edge.Services.Google.AdWords
{
    public class RetrieverService : PipelineService
    {
		#region Consts
		private const int MAX_ERROR_LENGTH = 4096;
		private const string QUERY_REPORT_URL_FORMAT = "{0}/api/adwords/reportdownload/{1}?" + "__fmt={2}";
		private const string REPORT_VERSION = "v201302";
                    
		#endregion

        #region Data Members
        private BatchDownloadOperation _batchDownloadOperation;
	    #endregion

		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			_batchDownloadOperation = new BatchDownloadOperation { MaxConcurrent = 1 };
			_batchDownloadOperation.Progressed += _batchDownloadOperation_Progressed;
			var includeZeroImpression = Boolean.Parse(Delivery.Parameters["includeZeroImpression"].ToString());

			// time period
			var startDate = Delivery.TimePeriodDefinition.Start.ToDateTime().ToString("yyyyMMdd");
			var endDate = Delivery.TimePeriodDefinition.End.ToDateTime().ToString("yyyyMMdd");

			foreach (var clientId in (string[])Delivery.Parameters["AdwordsClientIDs"])
			{
				//Get all files on specific client
				var files = Delivery.Files.Where(x => x.Parameters["AdwordsClientID"].ToString() == clientId);

				//Setting Adwords User
				var headers = new Dictionary<string, string>
						{
							{"DeveloperToken" ,Delivery.Parameters["DeveloperToken"].ToString()},
							{"UserAgent" , FileManager.UserAgentString},
							{"EnableGzipCompression","true"},
							{"ClientCustomerId",clientId},
							{"Email",Delivery.Parameters["MccEmail"].ToString()}
						};

				var user = new AdWordsUser(headers);
				// AuthToken
				var config = user.Config as AdWordsAppConfig;
				if (config == null)
					throw new Exception("Failed to convert AdwordUser.Config to AdWordsAppConfig");
				config.AuthToken = AdwordsUtill.GetAuthToken(user);

				var firstCheck = true;
				var awqls = new List<string>();

				foreach (var file in files)
				{
					// report type
					GA201302.ReportDefinitionReportType reportType;
					if (Enum.IsDefined(typeof(GA201302.ReportDefinitionReportType), file.Parameters["ReportType"].ToString()))
						reportType = (GA201302.ReportDefinitionReportType)Enum.Parse(typeof(GA201302.ReportDefinitionReportType), file.Parameters["ReportType"].ToString(), true);
					else
						throw new ConfigurationErrorsException(String.Format("Unknown Google Adwords Report Type '{0}'", file.Parameters["ReportType"]));

					// Creating AWQL
					var sb = new StringBuilder();
					sb.AppendFormat("SELECT {0} FROM {1}", file.Parameters["ReportFields"], file.Parameters["ReportType"]);

					if (!includeZeroImpression)
						sb.Append(" WHERE Impressions > 0");

					sb.AppendFormat(" DURING {0},{1}", startDate, endDate);

					var format = GA201302.DownloadFormat.GZIPPED_CSV.ToString();
					file.SourceUrl = string.Format(QUERY_REPORT_URL_FORMAT, config.AdWordsApiServer, REPORT_VERSION, format);
					var query = sb.ToString();
					var postData = string.Format("__rdquery={0}", HttpUtility.UrlEncode(query));

					awqls.Add(query);

					//Validate Report
					if (firstCheck)
					{
						string error;
						if (!ValidateReport(file, user, postData, out error))
						{
							//CHEKING FOR INVALID AUTHTOKEN
							if (error.Contains(GA201302.AuthenticationErrorReason.GOOGLE_ACCOUNT_COOKIE_INVALID.ToString()))
							{
								//RENEWING AUTHTOKEN
								config.AuthToken = AdwordsUtill.GetAuthToken(user, generateNew: true);
							}
							else throw new Exception(String.Format("Google Adwords API Error: {0}", error));
						}
						firstCheck = false;
					}

					//If Validate - Success
					DownloadFile(file, user, postData);
				}
			}

			Progress = 0.2;
			_batchDownloadOperation.Start();
			_batchDownloadOperation.Wait();

			_batchDownloadOperation.EnsureSuccess(); //INCASE OF GENERAL EXCEPTION OPEN DELIVERY FILE HAS HTML AND VIEW INNER ERROR

			Progress = 0.9;
			Delivery.Save();
			return ServiceOutcome.Success;
		} 
		#endregion

		#region Private Methods
		private void DownloadFile(DeliveryFile file, AdWordsUser user, string postData)
		{
			var request = CreateAdwordsReportRequest(file, user, postData);
			FileDownloadOperation operation = file.Download(request);
			operation.RequestBody = postData;

			_batchDownloadOperation.Add(operation);
		}

		private WebRequest CreateAdwordsReportRequest(DeliveryFile file, AdWordsUser user, string postBody)
		{
			var config = user.Config as AdWordsAppConfig;
			var request = WebRequest.Create(file.SourceUrl) as HttpWebRequest;

			if (config == null || request == null)
				return null;

			request.Timeout = 100000;
			if (!string.IsNullOrEmpty(postBody))
			{
				request.Method = "POST";
			}
			if (!string.IsNullOrEmpty(config.ClientCustomerId))
			{
				request.Headers.Add("clientCustomerId: " + (user.Config as AdWordsAppConfig).ClientCustomerId);
			}

			request.ContentType = "application/x-www-form-urlencoded";

			if (config.EnableGzipCompression)
			{
				request.AutomaticDecompression = DecompressionMethods.GZip
					| DecompressionMethods.Deflate;
			}
			else
			{
				request.AutomaticDecompression = DecompressionMethods.None;
			}

			if (config.AuthorizationMethod == AdWordsAuthorizationMethod.ClientLogin)
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

		private void _batchDownloadOperation_Progressed(object sender, EventArgs e)
		{
			//BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
			//this.ReportProgress(DownloadOperation.Progress);
		}

		private bool DownloadReportToStream(string downloadUrl, AdWordsAppConfig config, bool returnMoneyInMicros, Stream outputStream, string postBody, AdWordsUser user)
		{
			var request = (HttpWebRequest)WebRequest.Create(downloadUrl);
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
				request.AutomaticDecompression = DecompressionMethods.GZip
					| DecompressionMethods.Deflate;
			}
			else
			{
				request.AutomaticDecompression = DecompressionMethods.None;
			}
			if (config.AuthorizationMethod == AdWordsAuthorizationMethod.OAuth2)
			{
				if (user.OAuthProvider != null)
				{
					request.Headers["Authorization"] = user.OAuthProvider.GetAuthHeader(downloadUrl);
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
				using (var writer = new StreamWriter(request.GetRequestStream()))
				{
					writer.Write(postBody);
				}
			}

			// AdWords API now returns a 400 for an API error.
			bool retval;
			WebResponse response;
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

		private bool ValidateReport(DeliveryFile file, AdWordsUser user, string postData, out string errorMsg)
		{
			errorMsg = string.Empty;
			WebRequest request = CreateAdwordsReportRequest(file, user, postData);
			request.Proxy = null;
			request.Headers.Add("validateOnly: true"); // Add Validate

			using (var writer = new StreamWriter(request.GetRequestStream()))
			{
				writer.Write(postData);
			}

			//Try to ger response from server
			WebResponse response;
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
					// BUG fix due to adwords new version changes
					//if (Regex.IsMatch(previewString, REPORT_ERROR_REGEX))
					if ((previewString.Contains(typeof(GA201302.AuthorizationError).Name)) || (previewString.Contains(typeof(GA201302.AuthenticationError).Name)))
					{
						errorMsg = previewString;
						return false;
					}
					throw new Exception(previewString);
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
			var decoder = Encoding.UTF8.GetDecoder();
			var charArray = new char[previewBytes.Length];
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

			const int bufferSize = 2 << 20;
			var buffer = new byte[bufferSize];
			var byteArray = new List<byte>();

			int bytesRead;
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
		#endregion
    }
}
