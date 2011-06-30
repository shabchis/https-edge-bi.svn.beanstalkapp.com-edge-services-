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

namespace Edge.Services.Google.Adwords
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

		public GoogleUserEntity(string mccEmail,string accountEmail)
		{
			this._mccEmail = mccEmail;
			this._accountEmail = accountEmail;
			this._authToken = GetAuthToken(mccEmail);
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = _authToken,
				DeveloperToken = _developerToken,
				ApplicationToken = _applicationToken,

				ClientEmail = accountEmail,
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public GoogleUserEntity(string _email, string _authToken, string _developerToken = "5eCsvAOU06Fs4j5qHWKTCA",
			string _applicationToken = "5eCsvAOU06Fs4j5qHWKTCA", string userAgent = "Edge.BI", bool enableGzipCompression = true)
		{
			this._accountEmail = _email;
			this._authToken = _authToken;
			this._developerToken = _developerToken;

			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = _authToken,
				DeveloperToken = _developerToken,
				ApplicationToken = _applicationToken,
				ClientEmail = _accountEmail,
				UserAgent = userAgent,
				EnableGzipCompression = enableGzipCompression
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public string GetAuthToken(string mccEmail)
		{
			string auth = GetAuthFromDB(mccEmail);
			if (string.IsNullOrEmpty(auth))
				auth = GetAuthFromApi(mccEmail,this._mccPass);

			return auth;
		}

		private string GetAuthFromApi(string mccEmail,string pass)
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
			AdWordsUser user = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
			var reportService = (ReportDefinitionService)user.GetService(AdWordsService.v201101.ReportDefinitionService);
			auth = reportService.RequestHeader.authToken;
			SetAuthToken(mccEmail, auth);

			return auth;
			
		}

		private void SetAuthToken(string mccEmail, string auth)
		{
			using (SqlConnection connection = new SqlConnection(AppSettings.GetConnectionString(this, "MCC_Auth")))
			{
				SqlCommand cmd = DataManager.CreateCommand(@"SetGoogleMccAuth(@MccEmail:Nvarchar,@AuthToken:Nvarchar)", System.Data.CommandType.StoredProcedure);
				cmd.Connection = connection;
				connection.Open();
				cmd.Parameters["@MccEmail"].Value = mccEmail;
				cmd.Parameters["@AuthToken"].Value = auth;
				cmd.ExecuteNonQuery();
			}
		}

		public string GetAuthFromDB(string mccEmail)
		{
			string auth = "";

			using (SqlConnection connection = new SqlConnection(AppSettings.GetConnectionString(this, "MCC_Auth")))
			{
				SqlCommand cmd = DataManager.CreateCommand(@"GetGoogleMccAuth(@MccEmail:Nvarchar)", System.Data.CommandType.StoredProcedure);
				cmd.Connection = connection;
				connection.Open();
				cmd.Parameters["@MccEmail"].Value = mccEmail;

				using (SqlDataReader reader = cmd.ExecuteReader())
				{
					while (reader.Read())
					{
						this._mccPass = Encryptor.Dec(reader[0].ToString());
						auth = reader[1].ToString();
					}
				}
			}
			return auth;
		}



	}

}
