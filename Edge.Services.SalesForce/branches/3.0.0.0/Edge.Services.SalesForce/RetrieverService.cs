using System;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Core.Configuration;
using System.Net;
using System.Threading;
using System.IO;
using Newtonsoft.Json;

namespace Edge.Services.SalesForce
{
	public class RetrieverService : PipelineService
	{
		#region Overrides
		protected override ServiceOutcome DoPipelineWork()
		{
			var mutex = new Mutex(false, "SalesForceRetriever");
			var batch = new BatchDownloadOperation();
			try
			{
				mutex.WaitOne();

				var token = GetAuthenticationToken();

				// exist
				foreach (var file in Delivery.Files)
				{
					var query = String.Format(file.Parameters["Query"].ToString(), Delivery.TimePeriodStart.Year, Delivery.TimePeriodStart.Month, Delivery.TimePeriodStart.Day);
					file.SourceUrl = String.Format("{0}/services/data/v20.0/query?q={1}", token.InstanceUrl, query);

					var request = (HttpWebRequest)WebRequest.Create(file.SourceUrl);
					request.Headers.Add("Authorization: OAuth " + token.AccessToken);
					var fileDownloadOperation = file.Download(request);
					batch.Add(fileDownloadOperation);
				}
				batch.Start();
				batch.Wait();
				batch.EnsureSuccess();
			}
			finally
			{
				mutex.ReleaseMutex();
			}

			Delivery.Save();
			return ServiceOutcome.Success;
		} 
		#endregion

		#region Authentication
		private Token GetAuthenticationToken()
		{
			var tokenResponse = Token.Get(Delivery.Parameters["SalesForceClientID"].ToString());

			//if not exist
			if (string.IsNullOrEmpty(tokenResponse.AccessToken) || (string.IsNullOrEmpty(tokenResponse.RefreshToken)))
				tokenResponse = GetAccessTokenParamsFromSalesForce();

			//check if access_token is not expired
			if (tokenResponse.UpdateTime.Add((TimeSpan.Parse(AppSettings.Get(tokenResponse, "TimeOut")))) < DateTime.Now)
				tokenResponse = RefreshToken(tokenResponse.RefreshToken);

			return tokenResponse;
		}

		private Token RefreshToken(string refreshToken)
		{
			var myRequest = (HttpWebRequest)WebRequest.Create(Delivery.Parameters["AuthenticationUrl"].ToString());
			myRequest.Method = "POST";
			myRequest.ContentType = "application/x-www-form-urlencoded";

			using (var writer = new StreamWriter(myRequest.GetRequestStream()))
			{
				writer.Write(string.Format("refresh_token={0}&client_id={1}&client_secret={2}&grant_type=refresh_token",
					refreshToken,
					Delivery.Parameters["SalesForceClientID"],
					Delivery.Parameters["ClientSecret"]));
			}

			var myResponse = (HttpWebResponse)myRequest.GetResponse();
			var responseBody = myResponse.GetResponseStream();
			var encode = Encoding.GetEncoding("utf-8");
			var readStream = new StreamReader(responseBody, encode);

			var tokenResponse = (Token)JsonConvert.DeserializeObject(readStream.ReadToEnd(), typeof(Token));
			tokenResponse.RefreshToken = refreshToken;
			tokenResponse.UpdateTime = DateTime.Now;
			tokenResponse.Save(Delivery.Parameters["SalesForceClientID"].ToString());

			return tokenResponse;
		}

		private Token GetAccessTokenParamsFromSalesForce()
		{
			var myRequest = (HttpWebRequest)WebRequest.Create(Delivery.Parameters["AuthenticationUrl"].ToString());
			myRequest.Method = "POST";
			myRequest.ContentType = "application/x-www-form-urlencoded";

			using (var writer = new StreamWriter(myRequest.GetRequestStream()))
			{
				writer.Write(String.Format("code={0}&grant_type=authorization_code&client_id={1}&client_secret={2}&redirect_uri={3}",
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
				using (var reader = new StreamReader(webEx.Response.GetResponseStream()))
				{
					throw new Exception(reader.ReadToEnd());
				}
			}

			var responseBody = myResponse.GetResponseStream();
			var encode = Encoding.GetEncoding("utf-8");
			var readStream = new StreamReader(responseBody, encode);
			var token = JsonConvert.DeserializeObject<Token>(readStream.ReadToEnd());
			token.UpdateTime = DateTime.Now;
			token.Save(Delivery.Parameters["SalesForceClientID"].ToString());
			
			//return string itself (easier to work with)
			return token;
		} 
		#endregion
	}
}
