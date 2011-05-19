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
				AuthToken = "DQAAAJUAAAC7ij1fVVAQmRw4uuNEoldvGJSL-ndgOvfFgUwzMyACF4-JYOvqW75DQw_qoZmwX3FcPNkmp5slK-YVtmvb_7oLNYtImccG0yFp0E3TOcczVt0_7bKM82myTrN0kOyZQ6sVeMNjd2tuQQQUNij09yoVgn8qaRJvN_ieefjGJzItwZlK9O__AjdPXejbfuycNRJLcN-oG4NA3vdeUFq940hR",
				DeveloperToken = "5eCsvAOU06Fs4j5qHWKTCA",
				ApplicationToken = "5eCsvAOU06Fs4j5qHWKTCA",

				//ClientEmail = "bezeqaccess@gmail.com",
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public void SetGoogleUserEntity()
		{
			AdWordsAppConfig config = new AdWordsAppConfig()
			{
				AuthToken = authToken,
				DeveloperToken = developerToken,
				ApplicationToken = applicationToken,
				//ClientEmail = "bezeqaccess@gmail.com",
				UserAgent = "Edge.BI",
				EnableGzipCompression = true
			};
			adwordsUser = new AdWordsUser(new AdWordsServiceFactory().ReadHeadersFromConfig(config));
		}

		public AdWordsUser adwordsUser { set; get; }

		// TO DO : get the following from configuration 
		private string authToken { set; get; }
		private string developerToken { set; get; }
		private string applicationToken { set; get; }
		
	}

}
