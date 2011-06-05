using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Api.Ads.AdWords.Lib;

namespace Edge.Services.Google.Adwords
{
	public class GoogleUserEntity
	{
		public GoogleUserEntity()
		{
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = authToken,
				DeveloperToken = developerToken,
				ApplicationToken = applicationToken,
				ClientEmail = "Demo@gmail.com",
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public GoogleUserEntity(string email)
		{
			this.email = email;
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = "DQAAAKcAAABydlwYMHZJUf41AsS0_a8XalhHEtgpAFdfsTbPYZ8pQzAPyXz9ryljZHg5itfMR_VSpv98bEpo5lonR3gYjaDOI8aiUeVDmNAPoriyCMmMIRK3eMWBCJRoOsNAVwePdADWSx9OXLFRqAhnW_DbWhAsPHMLSchSDjyjQis7P7MtHmtFx-nuBQPy5cQzJsp2oBISlyzTIG13hEzGeCpmiXLbJ9iHoKhqdYktqIyGg1XcWw",
				DeveloperToken = "5eCsvAOU06Fs4j5qHWKTCA",
				ApplicationToken = "5eCsvAOU06Fs4j5qHWKTCA",

				ClientEmail = email,
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public GoogleUserEntity(string _email, string _authToken, string _developerToken = "5eCsvAOU06Fs4j5qHWKTCA",
			string _applicationToken = "5eCsvAOU06Fs4j5qHWKTCA", string userAgent = "Edge.BI", bool enableGzipCompression = true)
		{
			this.email = _email;
			this.authToken = _authToken;
			this.developerToken = _developerToken;

			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = authToken,
				DeveloperToken = developerToken,
				ApplicationToken = applicationToken,
				ClientEmail = email,
				UserAgent = userAgent,
				EnableGzipCompression = enableGzipCompression
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public AdWordsUser adwordsUser { set; get; }
		public string email { set; get; }
		// TO DO : get the following from configuration 
		
		private string authToken { set; get; }
		private string developerToken { set; get; }
		private string applicationToken { set; get; }
		
	}

}
