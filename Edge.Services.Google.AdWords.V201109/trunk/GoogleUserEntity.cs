using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Api.Ads.AdWords.Lib;
using System.Data.SqlClient;
using Edge.Core.Data;
using Edge.Core.Configuration;
using Google.Api.Ads.AdWords.v201109;
using Edge.Core.Utilities;
using Google.Api.Ads.Common.Lib;

namespace Edge.Services.Google.AdWords
{
	public class GoogleUserEntity
	{

		public AdWordsUser AdwordsUser { set; get; }
		private string _authToken { set; get; }
		private string _developerToken = "5eCsvAOU06Fs4j5qHWKTCA";
		private string _applicationToken = "5eCsvAOU06Fs4j5qHWKTCA";
		private string _mccPass { set; get; }
		private string _mccEmail { set; get; }
		private string _accountEmail { set; get; }
		public string AdwordsClientId { set; get; }


		public GoogleUserEntity(string mccEmail, string adwordsClientId, bool newAuth = false, string authConnectionString = "")
		{
			if (String.IsNullOrWhiteSpace(mccEmail) || String.IsNullOrWhiteSpace(adwordsClientId))
				throw new Exception(String.Format("{0} says: Invalid arguments", this));


			this._mccEmail = mccEmail;

			this.AdwordsClientId = adwordsClientId;
			//this._authToken = GetAuthToken(mccEmail, newAuth, authConnectionString);
			this._authToken = GetAuthFromApi(mccEmail, "mccpass2012", authConnectionString);


			Dictionary<string, string> headers = new Dictionary<string, string>()
			{
				{"AuthToken",_authToken},
				{"DeveloperToken" , _developerToken},
				{"ApplicationToken" , _applicationToken},
				{"UserAgent" , "Edge.BI"},
				{"EnableGzipCompression","true"}
			};

			this.AdwordsUser = new AdWordsUser(headers);
		}

		private string GetAuthToken(string mccEmail, bool newAuth, string authConnectionString)
		{
			string pass;
			string auth = GetAuthFromDB(mccEmail, authConnectionString, out pass);
			this._mccPass = pass;
			if (newAuth || string.IsNullOrEmpty(auth))
				auth = GetAuthFromApi(mccEmail, pass, authConnectionString);
			return auth;
		}

		/// <summary>
		/// Getting New Auth key From Google
		/// </summary>
		/// <param name="mccEmail">MCC Email - Edge / Seperia</param>
		/// <param name="pass">Deciphered password</param>
		/// <param name="authConnectionString"></param>
		/// <returns>Auth string</returns>
		private string GetAuthFromApi(string mccEmail, string pass, string authConnectionString)
		{
			string auth;

			Dictionary<string, string> headers = new Dictionary<string, string>()
			{
				{"DeveloperToken" , _developerToken},
				{"ApplicationToken" , _applicationToken},
				{"Email" , mccEmail},
				{"Password" ,pass},
				{"UserAgent" , "Edge.BI"},
				{"EnableGzipCompression","true"},
				{"ClientCustomerId",this.AdwordsClientId}
			};

			AdWordsUser user = new AdWordsUser(headers);
			this.AdwordsUser = user;

			try
			{
				
				//var reportService = (ReportDefinitionService)user.GetService(AdWordsService.v201109.ReportDefinitionService);
				string authToken = new AuthToken((AdWordsAppConfig)user.Config, AdWordsSoapClient.SERVICE_NAME, (user.Config as AdWordsAppConfig).Email,
			  (user.Config as AdWordsAppConfig).Password).GetToken();



				auth = authToken;
				//SetAuthToken(mccEmail, auth, authConnectionString);
			}
			catch (Exception ex)
			{
				Log.Write("Error while trying to create new Auth key", ex);
				throw new Exception("Error while trying to create new Auth key", ex);
			}

			return auth;

		}

		/// <summary>
		/// Saving Auth in DataBase for reusing.
		/// </summary>
		private void SetAuthToken(string mccEmail, string auth, string authConnectionString)
		{
			SqlConnection connection;

			if (string.IsNullOrEmpty(authConnectionString))
			{
				connection = new SqlConnection(AppSettings.GetConnectionString(this, "MCC_Auth"));
			}
			else
				connection = new SqlConnection(authConnectionString);

			try
			{
				using (connection)
				{
					SqlCommand cmd = DataManager.CreateCommand(@"SetGoogleMccAuth(@MccEmail:Nvarchar,@AuthToken:Nvarchar)", System.Data.CommandType.StoredProcedure);
					cmd.Connection = connection;
					connection.Open();
					cmd.Parameters["@MccEmail"].Value = mccEmail;
					cmd.Parameters["@AuthToken"].Value = auth;
					cmd.ExecuteNonQuery();
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error while trying to set a new Auth key", ex);
			}

		}

		private string GetAuthFromDB(string mccEmail, string authConnectionString, out string mccPassword)
		{
			string auth = "";
			mccPassword = "";
			SqlConnection connection;

			if (string.IsNullOrEmpty(authConnectionString))
			{
				connection = new SqlConnection(AppSettings.GetConnectionString(this, "MCC_Auth"));
			}
			else
				connection = new SqlConnection(authConnectionString);

			//	connection = new SqlConnection("Data Source=shayba-pc; Database=Edge_System; User ID=sa; Password=sbarchen");

			try
			{
				using (connection)
				{
					SqlCommand cmd = DataManager.CreateCommand(@"GetGoogleMccAuth(@MccEmail:Nvarchar)", System.Data.CommandType.StoredProcedure);
					cmd.Connection = connection;
					connection.Open();
					cmd.Parameters["@MccEmail"].Value = mccEmail;

					using (SqlDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							mccPassword = Encryptor.Dec(reader[0].ToString());
							auth = reader[1].ToString();
						}
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error while trying to get auth key from DB", ex);
			}

			return auth;
		}


	}

}
