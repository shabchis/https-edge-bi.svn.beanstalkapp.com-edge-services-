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

namespace Edge.Services.Google.AdWords
{
	class RetrieverService : PipelineService
	{

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
							{"Email",this.Delivery.Parameters["MccEmail"].ToString()},
							{"Password",this.Delivery.Parameters["MccPass"].ToString()}
						};

				AdWordsUser user = new AdWordsUser(headers);

				//Downloading Files
				foreach (DeliveryFile file in files)
				{
					//if (file.Name.ToString().Equals(GoogleStaticReportsNamesUtill._reportNames[ReportDefinitionReportType.AD_PERFORMANCE_REPORT]))
					{
						//Creating Report Definition
						ReportDefinition definition = AdwordsUtill.CreateNewReportDefinition(file as DeliveryFile, startDate, endDate);
						
						string path = SetTargetLocation(file.CreateLocation());
						file.Location = path;
						//file.Save();
						new ReportUtilities(user).DownloadClientReport(definition, path);

						//SetDeliveryFileParams(file, user);
						//DownloadFile(file, user);
					}
				}

			}
			//_batchDownloadOperation.Start();
			//_batchDownloadOperation.Wait();
			//_batchDownloadOperation.EnsureSuccess();
			this.Delivery.Save();

			return Core.Services.ServiceOutcome.Success;

		}

		protected string SetTargetLocation(string targetLocation)
		{

			Uri uri;
			uri = FileManager.GetRelativeUri(targetLocation);

			// Get full path
			string fullPath = Path.Combine(AppSettings.Get(typeof(FileManager), "RootPath"), uri.ToString());
			if (!Directory.Exists(Path.GetDirectoryName(fullPath)))
				Directory.CreateDirectory(Path.GetDirectoryName(fullPath));

			return   fullPath;
		}

		private void SetDeliveryFileParams(DeliveryFile file, AdWordsUser user)
		{
			Dictionary<string, string> requestParams = AdwordsUtill.GetRequestParams(user);

			file.SourceUrl = requestParams["DownloadURL"]; ;
			foreach (var item in requestParams)
			{
				if (item.Key == "DownloadURL")
					continue;
				file.Parameters.Add(item.Key, item.Value);
			}


		}

		void _batchDownloadOperation_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(DownloadOperation.Progress);
		}


		private void DownloadFile(DeliveryFile file, AdWordsUser user)
		{

			#region Creating request
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);
			request.Timeout = 100000;
			if (!string.IsNullOrEmpty(file.Parameters["PostBody"].ToString()))
			{
				request.Method = file.Parameters["Method"].ToString();
			}

			if (!string.IsNullOrEmpty((user.Config as AdWordsAppConfig).ClientEmail))
			{
				request.Headers.Add(file.Parameters["ClientEmail"].ToString());
			}
			else if (!string.IsNullOrEmpty((user.Config as AdWordsAppConfig).ClientCustomerId))
			{
				request.Headers.Add(file.Parameters["ClientCustomerId"].ToString());
			}
			request.ContentType = file.Parameters["ContentType"].ToString();

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
				string authToken = (!string.IsNullOrEmpty((user.Config as AdWordsAppConfig).AuthToken)) ? (user.Config as AdWordsAppConfig).AuthToken :
					new AuthToken(
						(user.Config as AdWordsAppConfig), AdWordsSoapClient.SERVICE_NAME, (user.Config as AdWordsAppConfig).Email,
						(user.Config as AdWordsAppConfig).Password).GetToken();

				(user.Config as AdWordsAppConfig).AuthToken = authToken;
				//string authToken = AdwordsUtill.GetAuthToken(user);

				request.Headers["Authorization"] = "GoogleLogin auth=" + authToken;
			}

			request.Headers.Add(file.Parameters["ReturnMoneyInMicros"].ToString());
			request.Headers.Add("developerToken: " + this.Delivery.Parameters["DeveloperToken"].ToString());

			//byte[] bytes = Encoding.UTF8.GetBytes(file.Parameters["PostBody"].ToString());
			//request.ContentLength = bytes.Length;

			//using (var stream = request.GetRequestStream())
			//{
			//    stream.Write(bytes, 0, bytes.Length);
			//}

			if (!string.IsNullOrEmpty(file.Parameters["PostBody"].ToString()))
			{
				using (StreamWriter writer = new StreamWriter(request.GetRequestStream()))
				{
					writer.Write(file.Parameters["PostBody"].ToString());
				}
			}
			#endregion

			var response = (HttpWebResponse) request.GetResponse();
			
			_batchDownloadOperation.Add(file.Download(response.GetResponseStream(), response.ContentLength));


		}
	}
}
