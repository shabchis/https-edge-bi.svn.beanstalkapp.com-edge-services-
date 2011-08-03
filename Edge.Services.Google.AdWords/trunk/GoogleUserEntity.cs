using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Api.Ads.AdWords.Lib;
using System.Data.SqlClient;
using Edge.Core.Data;
using Edge.Core.Configuration;
using Google.Api.Ads.AdWords.v201101;
using Edge.Core.Utilities;

namespace Edge.Services.Google.AdWords
{
	public class GoogleUserEntity
	{

		public AdWordsUser adwordsUser { set; get; }


		private string _authToken { set; get; }
		private string _developerToken = "5eCsvAOU06Fs4j5qHWKTCA";
		private string _applicationToken = "5eCsvAOU06Fs4j5qHWKTCA";
		private string _mccPass { set; get; }
		private string _mccEmail { set; get; }
		public string _accountEmail { set; get; }
		public string AdwordsClientId { set; get; }


		public GoogleUserEntity()
		{
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = _authToken,
				DeveloperToken = _developerToken,
				ApplicationToken = _applicationToken,
				ClientEmail = "Demo@gmail.com",
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public GoogleUserEntity(string mccEmail, string adwordsClientId, bool newAuth = false, string authConnectionString = "")
		{
			if (String.IsNullOrWhiteSpace(mccEmail) || String.IsNullOrWhiteSpace(adwordsClientId))
				throw new Exception(String.Format("{0} says: Invalid arguments", this));
			
			
			this._mccEmail = mccEmail;
			//this._accountEmail = accountEmail;
			this.AdwordsClientId = adwordsClientId;
			this._authToken = GetAuthToken(mccEmail, newAuth, authConnectionString);
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = _authToken,
				DeveloperToken = _developerToken,
				ApplicationToken = _applicationToken,
				//ClientEmail = accountEmail,
				ClientCustomerId = this.AdwordsClientId,
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public string GetAuthToken(string mccEmail, bool newAuth, string authConnectionString)
		{
			string pass;
			string auth = GetAuthFromDB(mccEmail,authConnectionString,out pass);
			this._mccPass = pass;
			if (newAuth || string.IsNullOrEmpty(auth))
				auth = GetAuthFromApi(mccEmail, pass, authConnectionString);
			return auth;
		}

		private string GetAuthFromApi(string mccEmail, string pass, string authConnectionString)
		{
			string auth;
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				Email = mccEmail,
				Password = pass,
				DeveloperToken = _developerToken,
				ApplicationToken = _applicationToken,
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			try
			{
				AdWordsUser user = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
				var reportService = (ReportDefinitionService)user.GetService(AdWordsService.v201101.ReportDefinitionService);
				auth = reportService.RequestHeader.authToken;
				SetAuthToken(mccEmail, auth, authConnectionString);
			}
			catch (Exception ex)
			{
				Log.Write("Error while trying to create new Auth key", ex);
				throw new Exception("Error while trying to create new Auth key",ex);
			}

			return auth;

		}

		private void SetAuthToken(string mccEmail, string auth, string authConnectionString)
		{
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

		public string GetAuthFromDB(string mccEmail,string authConnectionString,out string mccPassword)
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
