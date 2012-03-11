using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Net;
using System.IO;
using Edge.Core.Services;
using Edge.Core.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using Edge.Core.Configuration;

namespace Edge.Services.Facebook.UpdateCampaignStatus
{
	public class UpdateCampaignStatus : PipelineService
	{
		protected string _urlAuth;
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			#region Init
			this.Delivery = this.NewDelivery();

			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
				OriginalID = this.Instance.Configuration.Options[FacebookConfigurationOptions.Account_ID].ToString()
			};

			Delivery.Channel = new Data.Objects.Channel()
			{
				ID = 6
			};

			Delivery.Signature = "UpdateCampaignStatus";
			Delivery.TargetLocationDirectory = "Not Relevant";
			var configOptionsToCopyToDelivery = new string[] {
				FacebookConfigurationOptions.Account_ID,
				FacebookConfigurationOptions.Account_Name,
				FacebookConfigurationOptions.Auth_ApiKey,
				FacebookConfigurationOptions.Auth_AppSecret,
				FacebookConfigurationOptions.Auth_SessionKey,
				FacebookConfigurationOptions.Auth_SessionSecret,
				FacebookConfigurationOptions.Auth_RedirectUri,
				FacebookConfigurationOptions.Auth_AuthenticationUrl
			};
			foreach (string option in configOptionsToCopyToDelivery)
				this.Delivery.Parameters[option] = this.Instance.Configuration.Options[option];
			if (string.IsNullOrEmpty(this.Instance.Configuration.Options[FacebookConfigurationOptions.BaseServiceAddress]))
				throw new Exception("facebook base url must be configured!");



			this.ReportProgress(0.2);
			Delivery.Save();
			#endregion

			#region Authentication
			_urlAuth = string.Format(string.Format(Delivery.Parameters[FacebookConfigurationOptions.Auth_AuthenticationUrl].ToString(), Delivery.Parameters[FacebookConfigurationOptions.Auth_ApiKey],
				Delivery.Parameters[FacebookConfigurationOptions.Auth_RedirectUri],
				Delivery.Parameters[FacebookConfigurationOptions.Auth_AppSecret],
				 Delivery.Parameters[FacebookConfigurationOptions.Auth_SessionSecret]));

			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(_urlAuth);
			WebResponse response = request.GetResponse();

			using (StreamReader stream = new StreamReader(response.GetResponseStream()))
			{
				this.Delivery.Parameters["AccessToken"] = stream.ReadToEnd();
			}



			#endregion


			#region UpdateCampaignStatus

			ServiceOutcome outcome = ServiceOutcome.Success;
			int hourOfDay = DateTime.Now.Hour;
			int today = Convert.ToInt32(DateTime.Now.DayOfWeek);
			if (today == 0)
				today = 7;


			Dictionary<long, int> statusByCampaignID = new Dictionary<long, int>();



			//prepere sqlstatment by time of day get all campaings by time and status != null and 1

			using (SqlConnection connection = new SqlConnection(AppSettings.GetConnectionString(this, "DB")))
			{
				connection.Open();

				SqlCommand sqlCommand = DataManager.CreateCommand(string.Format(@"SELECT distinct T2.campaignid,T0.Hour{0} 
																				FROM Campaigns_Scheduling T0
																				INNER JOIN User_GUI_Account T1 ON T0.Account_ID=T1.Account_ID
																				INNER JOIN UserProcess_GUI_PaidCampaign T2 ON T0.Campaign_GK=T2.Campaign_GK
																				WHERE T0.Day=@Day:Int AND T0.Account_ID=@Account_ID:Int 
																				AND (T0.Hour{0} =1 OR T0.Hour{0}=2) AND
																				T2.Channel_ID=@Channel_ID:Int AND T1.Status!=0 AND T2.campStatus<>3 AND T2.ScheduleEnabled=1", hourOfDay.ToString().PadLeft(2, '0')));
				sqlCommand.Parameters["@Day"].Value = today;
				sqlCommand.Parameters["@Account_ID"].Value = this.Instance.AccountID;
				sqlCommand.Parameters["@Channel_ID"].Value = Delivery.Channel.ID;
				sqlCommand.Connection = connection;


				using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
				{
					while (sqlDataReader.Read())
					{
						long campaign_ID = Convert.ToInt64(sqlDataReader[0]);
						int campaign_status = Convert.ToInt32(sqlDataReader[1]);
						statusByCampaignID.Add(campaign_ID, campaign_status);
					}
				}
			}


			StringBuilder errorBuilder = new StringBuilder();
			foreach (KeyValuePair<long, int> item in statusByCampaignID)
			{
				request = (HttpWebRequest)HttpWebRequest.Create(string.Format(@"https://graph.facebook.com/{0}?campaign_status={1}&{2}", item.Key, item.Value, Delivery.Parameters["AccessToken"].ToString()));
				request.Method = "POST";
				string strResponse;
				try
				{
					response = request.GetResponse();
					
					using (StreamReader stream = new StreamReader(response.GetResponseStream()))
					{
						 strResponse= stream.ReadToEnd();

					}
					if (strResponse != "true")
					{
						outcome = ServiceOutcome.Failure;
						errorBuilder.Append(strResponse);
					}					

				}
				catch (WebException ex)
				{
					using (StreamReader reader = new StreamReader(ex.Response.GetResponseStream()))
					{
						strResponse = reader.ReadToEnd();
						errorBuilder.Append(strResponse);
					}
					
				}

			}
			if (errorBuilder.Length > 0)
				Edge.Core.Utilities.Log.Write(errorBuilder.ToString(), Core.Utilities.LogMessageType.Error);


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


}

