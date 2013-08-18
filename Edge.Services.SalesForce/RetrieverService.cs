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
				Token tokenResponse = Token.Get(Delivery.Parameters["SalesForceClientID"].ToString());
				//if not exist
				if (string.IsNullOrEmpty(tokenResponse.access_token) || (string.IsNullOrEmpty(tokenResponse.refresh_token)))
					tokenResponse = GetAccessTokenParamsFromSalesForce();


				//check if access_token is not expired
				if (tokenResponse.UpdateTime.Add((TimeSpan.Parse(AppSettings.Get(tokenResponse,"TimeOut")))) < DateTime.Now)
					tokenResponse = RefreshToken(tokenResponse.refresh_token);


				#endregion
				// exist
				foreach (var file in Delivery.Files)
				{
                    string Lead_query = "SELECT Edge_BI_Tracker__c, Company_Type__c FROM Lead WHERE CreatedDate > 2013-06-28T00:00:00.00Z AND Edge_BI_Tracker__c != null ORDER BY CreatedDate DESC";
                   
                    //Getting Empty results from this query.
                    string Lead_From_Task_Q = "SELECT CreatedDate, Description,Subject, Status, Edge_BI_Tracker__c FROM Task WHERE CreatedDate > 2013-06-28T00:00:00.00Z  AND What.Type = 'Lead'";
                    
                    string Demo_query = "SELECT CreatedDate, Description,Subject, Status, Edge_BI_Tracker__c FROM Task WHERE CreatedDate > 2013-06-28T00:00:00.00Z  AND Subject = '**Demo**' AND What.Type = 'Account' AND Status LIKE 'Waiting on someone else'";
                    
                    //Check for closed date
                    string Completed_demo_query = "SELECT CreatedDate, Description,Subject, Status, Edge_BI_Tracker__c  FROM Task WHERE CreatedDate > 2013-06-28T00:00:00.00Z  AND Subject = '**Demo**' AND What.Type = 'Account' AND Status LIKE 'Completed'";
                   
                    //To do :
                    string Opportunity_query = "SELECT CreatedBy.CreatedDate,StageName,CloseDate,RecordTypeId FROM Opportunity WHERE CALENDAR_YEAR(CloseDate)=2013 AND CALENDAR_MONTH(CloseDate)=6 AND DAY_IN_MONTH(CloseDate) > 26 AND StageName IN('In Process','Pending Closing - No POC','Pending Closing - With POC','In POC,Renew/Expand','Stage equals SMB - Qualified','SMB - Tech Review','SMB - Procurment / Negotiation')";
                    string Closed_Deal_query = "SELECT StageName,CloseDate,RecordTypeId,Amount FROM Opportunity WHERE CALENDAR_YEAR(CloseDate)=2013 AND CALENDAR_MONTH(CloseDate)=6 AND DAY_IN_MONTH(CloseDate) > 26 AND StageName IN('Closed Won','SMB - Closed Won')";

                    //Tests
                    string test_closed_Opportunity = "SELECT StageName,CloseDate FROM Opportunity WHERE CALENDAR_YEAR(CloseDate)=2013 AND CALENDAR_MONTH(CloseDate)=6 AND DAY_IN_MONTH(CloseDate) > 26  AND StageName IN('In Process')LIMIT 100";
                    string test_closed_Opportunity2 = "SELECT What.Id ,(SELECT Id,Amount FROM Opportunity) FROM Task  WHERE Opportunity.Id = What.Id LIMIT 30";
                    string test_closed_Opportunity3 = "SELECT Id, What.Amount FROM Task WHERE What.Type ='Opportunity' LIMIT 30";


                    string query = Lead_query;

                    //Tests:
                   // string query = "SELECT (SELECT ActivityDate, Description,IsTask, Subject, Status  FROM OpenActivities WHERE ActivityDate ORDER BY ActivityDate DESC) FROM Account LIMIT 100";
                    //string query = "SELECT (SELECT ActivityDate,ActivityType, IsTask, Subject, Status FROM ActivityHistories WHERE Subject LIKE '**Demo**' AND Status LIKE 'Waiting on someone else'  ORDER BY ActivityDate DESC) FROM Account  LIMIT 100";
                   // string query = "SELECT Id FROM Account WHERE ((CALENDAR_YEAR(CreatedDate)=2013 AND CALENDAR_MONTH(CreatedDate)=8 AND DAY_IN_MONTH(CreatedDate)=7)";
                    //"SELECT CreatedDate,CloseDate Description,Subject, Status, Edge_BI_Tracker__c  FROM Task WHERE CloseDate > 2013-06-28T00:00:00.00Z  AND What.Type = 'Opportunity' LIMIT 100";
                    //CreatedDate, Description,Subject, Status, Edge_BI_Tracker__c,Amount
                    //string query = string.Format(file.Parameters["Query"].ToString(), Delivery.TimePeriodStart.Year, Delivery.TimePeriodStart.Month, Delivery.TimePeriodStart.Day);
					
					file.SourceUrl = string.Format("{0}/services/data/v20.0/query?q={1}",tokenResponse.instance_url, query);					
					
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
			tokenResponse.Save(Delivery.Parameters["SalesForceClientID"].ToString());
			return tokenResponse;
		}

		private Token GetAccessTokenParamsFromSalesForce()
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
				using (StreamReader reader=new StreamReader(webEx.Response.GetResponseStream()))
				{
					throw new Exception(reader.ReadToEnd());
					
				}
				
				
			}
			
			Stream responseBody = myResponse.GetResponseStream();
			Encoding encode = System.Text.Encoding.GetEncoding("utf-8");			
			StreamReader readStream = new StreamReader(responseBody, encode);			
			Token token = JsonConvert.DeserializeObject<Token>(readStream.ReadToEnd());
			token.UpdateTime = DateTime.Now;
			token.Save(this.Delivery.Parameters["SalesForceClientID"].ToString());
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
					command.Parameters["@UpdateTime"].Value = DateTime.Now;
					
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
							tokenResponse.UpdateTime = Convert.ToDateTime(reader["UpdateTime"]);
							tokenResponse.ClientID = clientID;
							tokenResponse.id=reader["Id"].ToString();
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
