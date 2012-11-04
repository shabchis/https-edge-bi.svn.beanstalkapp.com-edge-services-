using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using Google.Api.Ads.AdWords.v201109;
using Google.Api.Ads.AdWords.Util;
using System.Threading;
using Edge.Core.Utilities;
using Google.Api.Ads.AdWords.Lib;
using Google.Api.Ads.AdWords.Util.Reports;
using Google.Api.Ads.Common.Lib;
using System.IO;
using Edge.Core.Configuration;
using Google.Api.Ads.Common.Util;
using System.Web;
using System.Text.RegularExpressions;

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
		ReportDefinitionDateRangeType _dateRange;
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
			_dateRange = ReportDefinitionDateRangeType.CUSTOM_DATE;
			string startDate = this.TargetPeriod.Start.ToDateTime().ToString("yyyyMMdd");
			string endDate = this.TargetPeriod.End.ToDateTime().ToString("yyyyMMdd");
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
				//Downloading Files
				foreach (DeliveryFile file in files)
				{
						//Creating Report Definition
						ReportDefinition definition = AdwordsUtill.CreateNewReportDefinition(file as DeliveryFile, startDate, endDate);

						//Getting AuthToken
						(user.Config as AdWordsAppConfig).AuthToken = AdwordsUtill.GetAuthToken(user);
						//new ReportUtilities(user).DownloadClientReport(definition, path);

						file.SourceUrl = string.Format(AdwordsUtill.ADHOC_REPORT_URL_FORMAT, (user.Config as AdWordsAppConfig).AdWordsApiServer);

						//Validate Report
						if (firstCheck)
						{
							string error = string.Empty;
							if (!ValidateReport(file, user, definition, out error))
							{
								//CHEKING FOR INVALID AUTHTOKEN
								if (error.Contains(AuthenticationErrorReason.GOOGLE_ACCOUNT_COOKIE_INVALID.ToString()))
								{
									//RENEWING AUTHTOKEN
									(user.Config as AdWordsAppConfig).AuthToken = AdwordsUtill.GetAuthToken(user, generateNew: true);
								}
								else throw new Exception("Google Adwords API Error: " + error);
							}
							firstCheck = !firstCheck;
						}
						DownloadFile(file, user, definition);
					
				}

			}

			_batchDownloadOperation.Start();
			_batchDownloadOperation.Wait();

			_batchDownloadOperation.EnsureSuccess(); //INCASE OF GENERAL EXCEPTION OPEN DELIVERY FILE HAS HTML AND VIEW INNER ERROR

			this.Delivery.Save();

			return Core.Services.ServiceOutcome.Success;

		}

		private void DownloadFile(DeliveryFile file, AdWordsUser user, ReportDefinition reportDefinition)
		{

			WebRequest request = CreateAdwordsReportRequest(file, user, reportDefinition);
			FileDownloadOperation operation = file.Download(request);
			string postBody = "__rdxml=" + HttpUtility.UrlEncode(AdwordsUtill.ConvertDefinitionToXml(reportDefinition));
			operation.RequestBody = postBody;

			_batchDownloadOperation.Add(operation);

		}
		private WebRequest CreateAdwordsReportRequest(DeliveryFile file, AdWordsUser user, ReportDefinition reportDefinition)
		{
			WebRequest request = HttpWebRequest.Create(file.SourceUrl);
			//request.Timeout = 100000;
			if (!string.IsNullOrEmpty(AdwordsUtill.ConvertDefinitionToXml(reportDefinition)))
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
			return request;
		}
		private bool ValidateReport(DeliveryFile file, AdWordsUser user, ReportDefinition reportDefinition, out string ErrorMsg)
		{
			ErrorMsg = string.Empty;
			WebRequest request = CreateAdwordsReportRequest(file, user, reportDefinition);
			request.Proxy = null;
			request.Headers.Add("validateOnly: true"); // Add Validate

			string postBody = "__rdxml=" + HttpUtility.UrlEncode(AdwordsUtill.ConvertDefinitionToXml(reportDefinition));

			using (StreamWriter writer = new StreamWriter(request.GetRequestStream()))
			{
				writer.Write(postBody);
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
					if (Regex.IsMatch(previewString, REPORT_ERROR_REGEX))
					{
						ErrorMsg = previewString;
						return false;
					}
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
		void _batchDownloadOperation_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(DownloadOperation.Progress);
		}
	}
}
