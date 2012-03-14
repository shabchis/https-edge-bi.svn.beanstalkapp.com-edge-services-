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

namespace Edge.Services.Google.Analytics
{
	class RetrieverService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			Mutex mutex = new Mutex(false, "GoogleAnalyticsRetriver");
			BatchDownloadOperation batch = new BatchDownloadOperation();
			try
			{
				mutex.WaitOne();
				#region Authentication
				//get access token + refresh token from db (if exist)
				Auth2 oAuth2 = Auth2.Get(Delivery.Parameters["ClientID"].ToString());
				//if not exist
				if (string.IsNullOrEmpty(oAuth2.access_token) || (string.IsNullOrEmpty(oAuth2.refresh_token)))
					oAuth2 = GetAccessTokenParamsFromGoogleAnalytics();


				//check if access_token is not expired
				if (oAuth2.updateTime.AddSeconds(oAuth2.expires_in - 300) < DateTime.Now)
					oAuth2 = RefreshToken(oAuth2.refresh_token);


				#endregion
				// exist
				foreach (var file in Delivery.Files)
				{
					string urlEncoded = string.Format(file.SourceUrl, Uri.EscapeUriString(oAuth2.access_token));
					HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(urlEncoded);
					request.Headers.Add("Accept-Encoding", "gzip");
					request.UserAgent="winrar(gzip)";
					

					FileDownloadOperation fileDownloadOperation = file.Download(request);
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
			return Core.Services.ServiceOutcome.Success;
		}

		private Auth2 RefreshToken(string refreshToken)
		{
			HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(Delivery.Parameters["AuthenticationUrl"].ToString());
			myRequest.Method = "POST";
			myRequest.ContentType = "application/x-www-form-urlencoded";

			using (StreamWriter writer = new StreamWriter(myRequest.GetRequestStream()))
			{
				writer.Write(string.Format("refresh_token={0}&client_id={1}&client_secret={2}&grant_type=refresh_token",
					refreshToken,
					Delivery.Parameters["ClientID"],
					Delivery.Parameters["ClientSecret"]));
			}

			HttpWebResponse myResponse = (HttpWebResponse)myRequest.GetResponse();
			Stream responseBody = myResponse.GetResponseStream();

			Encoding encode = System.Text.Encoding.GetEncoding("utf-8");


			StreamReader readStream = new StreamReader(responseBody, encode);


			Auth2 oAuth2;
			oAuth2 = (Auth2)JsonConvert.DeserializeObject(readStream.ReadToEnd(), typeof(Auth2));
			oAuth2.refresh_token = refreshToken;
			oAuth2.Save(Delivery.Parameters["ClientID"].ToString());
			return oAuth2;
		}

		private Auth2 GetAccessTokenParamsFromGoogleAnalytics()
		{
			HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(Delivery.Parameters["AuthenticationUrl"].ToString());
			myRequest.Method = "POST";
			myRequest.ContentType = "application/x-www-form-urlencoded";

			using (StreamWriter writer = new StreamWriter(myRequest.GetRequestStream()))
			{
				writer.Write(string.Format("code={0}&client_id={1}&client_secret={2}&redirect_uri={3}&grant_type=authorization_code",
					Delivery.Parameters["ConsentCode"],
					Delivery.Parameters["ClientID"],
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
				using (StreamReader reader=new StreamReader(webEx.Response.GetResponseStream()))
				{
					throw new Exception(reader.ReadToEnd());
					
				}
				
				
			}
			
			Stream responseBody = myResponse.GetResponseStream();

			Encoding encode = System.Text.Encoding.GetEncoding("utf-8");


			StreamReader readStream = new StreamReader(responseBody, encode);
			Auth2 oAuth2 = (Auth2)JsonConvert.DeserializeObject(readStream.ReadToEnd(), typeof(Auth2));
			oAuth2.updateTime = DateTime.Now;
			oAuth2.Save(Delivery.Parameters["ClientID"].ToString());
			//return string itself (easier to work with)
			return oAuth2;
		}







		public EventHandler batch_Progressed { get; set; }
	}
	public class Auth2
	{

		public string access_token { get; set; }
		public string token_type { get; set; }
		public int expires_in { get; set; }
		public string id_token { get; set; }
		public string refresh_token { get; set; }
		public DateTime updateTime { get; set; }








		internal void Save(string clientID)
		{
			Auth2 oAuth = new Auth2();
			using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(oAuth, "DB")))
			{
				using (SqlCommand command = DataManager.CreateCommand(AppSettings.Get(typeof(Auth2), "SP_Save"), CommandType.StoredProcedure))
				{
					conn.Open();
					command.Connection = conn;
					command.Parameters["@ClientID"].Value = clientID;
					command.Parameters["@AccessToken"].Value = this.access_token;
					command.Parameters["@RefreshToken"].Value = this.refresh_token;
					command.Parameters["@ExpiresIn"].Value = this.expires_in;
					command.Parameters["@UpdateTime"].Value = DateTime.Now;
					command.ExecuteNonQuery();

				}
			}

		}

		internal static Auth2 Get(string clientID)
		{
			Auth2 oAuth = new Auth2();
			using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(oAuth, "DB")))
			{
				using (SqlCommand command = DataManager.CreateCommand(AppSettings.Get(typeof(Auth2), "SP_Get"), CommandType.StoredProcedure))
				{
					conn.Open();
					command.Connection = conn;
					command.Parameters["@clientID"].Value = clientID;
					using (SqlDataReader reader = command.ExecuteReader())
					{
						if (reader.HasRows)
						{
							reader.Read();
							oAuth.access_token = reader["AccessToken"].ToString();
							oAuth.refresh_token = reader["RefreshToken"].ToString();
							oAuth.expires_in = Convert.ToInt32(reader["ExpiresIn"]);
							oAuth.updateTime = Convert.ToDateTime(reader["UpdateTime"]);
						}
					}
				}
			}
			return oAuth;
		}
	}
}
