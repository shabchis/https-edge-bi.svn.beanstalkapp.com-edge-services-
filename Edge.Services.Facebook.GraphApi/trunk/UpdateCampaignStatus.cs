﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Core.Configuration;
using System.Net;
using System.IO;
using Edge.Core.Services;
using System.Data.SqlClient;
using Edge.Core.Data;
using Edge.Core.Utilities;




namespace Edge.Services.Facebook.GraphApi
{
	public class UpdateCampaignStatus : Service
	{
		string _urlAuth;
		string _redirectUri;
		string _apiKey;
		string _appSecret;

		protected override ServiceOutcome DoWork()
		{
			HttpWebRequest request;
			WebResponse response;
			#region Init

			Dictionary<int, FacbookAccountParams> facbookAccountParams = new Dictionary<int, FacbookAccountParams>();
			StringBuilder strAccounts = new StringBuilder();
			_urlAuth = this.Instance.Configuration.Options[FacebookConfigurationOptions.Auth_AuthenticationUrl];
			_redirectUri = this.Instance.Configuration.Options[FacebookConfigurationOptions.Auth_RedirectUri];
			_apiKey = this.Instance.Configuration.Options[FacebookConfigurationOptions.Auth_ApiKey];
			_appSecret = this.Instance.Configuration.Options[FacebookConfigurationOptions.Auth_AppSecret];
			foreach (AccountElement account in EdgeServicesConfiguration.Current.Accounts)
			{
				foreach (AccountServiceElement service in account.Services)
				{
					if (service.Uses.Element.Name == "Facebook.GraphApi")
					{
						facbookAccountParams.Add(account.ID, new FacbookAccountParams()
						{

							FacebookAccountID = service.Options[FacebookConfigurationOptions.Account_ID],
							SessionSecret = service.Options[FacebookConfigurationOptions.Auth_SessionSecret]
							//SessionKey = service.Options[FacebookConfigurationOptions.Auth_SessionKey]
						});
						strAccounts.AppendFormat("{0},", account.ID);
					}
				}
			}
			if (strAccounts.Length == 0)
			{
				Log.Write("No account runing facebook found", LogMessageType.Information);
				return ServiceOutcome.Success;
			}
			else //remove last ','
			{
				strAccounts.Remove(strAccounts.Length - 1, 1);
			}






			if (string.IsNullOrEmpty(this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress]))
				throw new Exception("facebook base url must be configured!");



			this.ReportProgress(0.2);

			#endregion




			#region UpdateCampaignStatus

			ServiceOutcome outcome = ServiceOutcome.Success;
			int hourOfDay = DateTime.Now.Hour;
			int today = Convert.ToInt32(DateTime.Now.DayOfWeek);
			if (today == 0)
				today = 7;


			Dictionary<int, Dictionary<long, int>> statusByCampaignID = new Dictionary<int, Dictionary<long, int>>();



			//prepere sqlstatment by time of day get all campaings by time and status != null and 1

			using (SqlConnection connection = new SqlConnection(AppSettings.GetConnectionString(this, "DB")))
			{
				connection.Open();

				SqlCommand sqlCommand = DataManager.CreateCommand(string.Format(@"SELECT distinct T0.Account_ID, T2.campaignid,T0.Hour{1} 
																				FROM Campaigns_Scheduling T0
																				INNER JOIN User_GUI_Account T1 ON T0.Account_ID=T1.Account_ID
																				INNER JOIN UserProcess_GUI_PaidCampaign T2 ON T0.Campaign_GK=T2.Campaign_GK
																				WHERE T0.Day=@Day:Int AND T0.Account_ID IN ({0}) 
																				AND (T0.Hour{1} =1 OR T0.Hour{1}=2) AND
																				T2.Channel_ID=6 AND T1.Status!=0 AND T2.campStatus<>3 AND T2.ScheduleEnabled=1 
																				ORDER BY T0.Account_ID", strAccounts.ToString(), hourOfDay.ToString().PadLeft(2, '0')));
				sqlCommand.Parameters["@Day"].Value = today;
				sqlCommand.Connection = connection;


				using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
				{
					while (sqlDataReader.Read())
					{
						int account_id = Convert.ToInt32(sqlDataReader["Account_ID"]);
						long campaign_ID = Convert.ToInt64(sqlDataReader["campaignid"]);
						int campaign_status = Convert.ToInt32(sqlDataReader[string.Format("Hour{0}", hourOfDay.ToString().PadLeft(2, '0'))]);
						if (!statusByCampaignID.ContainsKey(account_id))
							statusByCampaignID.Add(account_id, new Dictionary<long, int>());
						statusByCampaignID[account_id][campaign_ID] = campaign_status;
					}
				}
			}


			
			foreach (KeyValuePair<int, Dictionary<long, int>> byAccount in statusByCampaignID)
			{

				#region Authentication
				string accessToken = null;
				FacbookAccountParams param = facbookAccountParams[byAccount.Key];

				_urlAuth = string.Format(string.Format(_urlAuth, _apiKey,
						_redirectUri,
						_appSecret,
						param.SessionSecret));

				request = (HttpWebRequest)HttpWebRequest.Create(_urlAuth);
				try
				{
					response = request.GetResponse();
					using (StreamReader stream = new StreamReader(response.GetResponseStream()))
					{
						accessToken = stream.ReadToEnd();
					}
				}
				catch (WebException ex)
				{
					using (StreamReader reader = new StreamReader(ex.Response.GetResponseStream()))
					{
						outcome = ServiceOutcome.Failure;
						Edge.Core.Utilities.Log.Write(string.Format("Account {0} failed during authentication -{1}", byAccount.Key, reader.ReadToEnd()),ex,LogMessageType.Error,byAccount.Key);
					}

				}





				#endregion


				foreach (var byCampaign in statusByCampaignID[byAccount.Key])
				{
					request = (HttpWebRequest)HttpWebRequest.Create(string.Format(@"https://graph.facebook.com/{0}?campaign_status={1}&{2}", byCampaign.Key, byCampaign.Value, accessToken));
					request.Method = "POST";
					string strResponse;
					try
					{
						response = request.GetResponse();

						using (StreamReader stream = new StreamReader(response.GetResponseStream()))
						{
							strResponse = stream.ReadToEnd();

						}
						if (strResponse != "true")
						{
							outcome = ServiceOutcome.Failure;
							Edge.Core.Utilities.Log.Write(string.Format("Account {0} failed:{1}", byAccount.Key, strResponse),null,LogMessageType.Error,byAccount.Key);
						}
						else
							Edge.Core.Utilities.Log.Write(string.Format("Account- {0} with campaign-{1} updated successfuly with value {2} for day num{3}", byAccount.Key, byCampaign.Key, byCampaign.Value, today), null, LogMessageType.Error, byAccount.Key);

					}
					catch (WebException ex)
					{
						using (StreamReader reader = new StreamReader(ex.Response.GetResponseStream()))
						{
							outcome = ServiceOutcome.Failure;
							strResponse = reader.ReadToEnd();
							Edge.Core.Utilities.Log.Write(string.Format("Account {0} -{1}", byAccount.Key, strResponse),ex,LogMessageType.Error,byAccount.Key);
						}

					}
				}

			}
			

			#endregion
			return outcome;
		}





	}
	public class content
	{
		public List<batch> batch { get; set; }
		public string access_token { get; set; }
	}
	public class batch
	{
		public string method { get; set; }
		public string relative_url { get; set; }
		public string body { get; set; }

	}
	public class FacbookAccountParams
	{

		public string FacebookAccountID { get; set; }
		public string SessionSecret { get; set; }


	}

}
