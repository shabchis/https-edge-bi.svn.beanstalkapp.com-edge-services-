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

namespace Edge.Services.SalesForceApi
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
				Token tokenResponse = Token.Get(Delivery.Account.OriginalID);
				//if not exist
				if (string.IsNullOrEmpty(tokenResponse.access_token) || (string.IsNullOrEmpty(tokenResponse.refresh_token)))
					tokenResponse = GetAccessTokenParamsFromSalesForce();


				//check if access_token is not expired
				if (tokenResponse.issued_at.Add((TimeSpan.Parse(AppSettings.Get(tokenResponse,"TimeOutInMin")))) < DateTime.Now)
					tokenResponse = RefreshToken(tokenResponse.refresh_token);


				#endregion
				// exist
				foreach (var file in Delivery.Files)
				{
					file.SourceUrl = string.Format("{0}/services/data/v20.0/query?q={1}", tokenResponse.instance_url,file.Parameters["Query"]);
					
					//string urlEncoded = string.Format(file.SourceUrl, Uri.EscapeUriString(tokenResponse.access_token));
					HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.SourceUrl);
					request.Headers.Add("Authorization: OAuth " + tokenResponse.access_token);
					

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

		private Token RefreshToken(string refreshToken)
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


			Token tokenResponse;
			tokenResponse = (Token)JsonConvert.DeserializeObject(readStream.ReadToEnd(), typeof(Token));
			tokenResponse.refresh_token = refreshToken;
			tokenResponse.Save(Delivery.Account.OriginalID);
			return tokenResponse;
		}

		private Token GetAccessTokenParamsFromSalesForce()
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
			Token oAuth2 = (Token)JsonConvert.DeserializeObject(readStream.ReadToEnd(), typeof(Token));
			
			oAuth2.Save(Delivery.Account.OriginalID);
			//return string itself (easier to work with)
			return oAuth2;
		}







		public EventHandler batch_Progressed { get; set; }
	}
	public class Token
	{
		public string id { get; set; }
		public DateTime issued_at { get; set; }
		public string refresh_token { get; set; }
		public string instance_url { get; set; }
		public string signature { get; set; }
		public string access_token { get; set; }
		public string ClientID { get; set; }

		internal void Save(string clientID)
		{
			
			Token tokenResponse = new Token();
			
			using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(tokenResponse, "DB")))
			{
				using (SqlCommand command = DataManager.CreateCommand(AppSettings.Get(typeof(Token), "SP_Save"), CommandType.StoredProcedure))
				{
					conn.Open();
					command.Connection = conn;
					command.Parameters["@Id"].Value=this.id;
					command.Parameters["@ClientID"].Value = clientID;
					command.Parameters["@Instance_url"].Value=this.instance_url;					
					command.Parameters["@AccessToken"].Value = this.access_token;
					command.Parameters["@RefreshToken"].Value = this.refresh_token;
					command.Parameters["@Signature"].Value=this.signature;
					command.Parameters["@Issued_at"].Value = this.issued_at;
					
					command.ExecuteNonQuery();

				}
			}

		}

		internal static Token Get(string clientID)
		{
			Token tokenResponse = new Token();
			using (SqlConnection conn = new SqlConnection(AppSettings.GetConnectionString(tokenResponse, "DB")))
			{
				using (SqlCommand command = DataManager.CreateCommand(AppSettings.Get(typeof(Token), "SP_Get"), CommandType.StoredProcedure))
				{
					conn.Open();
					command.Connection = conn;
					command.Parameters["@ClientID"].Value = clientID;
					using (SqlDataReader reader = command.ExecuteReader())
					{
						if (reader.HasRows)
						{
							reader.Read();
							tokenResponse.ClientID = clientID;
							tokenResponse.id=reader["Id"].ToString();
							tokenResponse.access_token = reader["AccessToken"].ToString();
							tokenResponse.issued_at =Convert.ToDateTime(reader["Issued_at"]);
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
