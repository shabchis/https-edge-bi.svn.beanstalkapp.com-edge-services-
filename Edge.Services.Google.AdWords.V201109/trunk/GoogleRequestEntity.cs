using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Google.AdWords
{
	public class GoogleRequestEntity
	{
		public GoogleRequestEntity()
		{

		}
		public GoogleRequestEntity(string Url, string Id, string Email, string AuthToken, bool IsReturnMoneyInMicros, string DeveloperToken, string Body, string EnableGzipCompression)
		{
			this.DownloadUrl = Url;
            this.ClientCustomerId = Id;
            this.ClientEmail = Email;
            this.AuthToken = AuthToken;
            this.ReturnMoneyInMicros = IsReturnMoneyInMicros.ToString().ToLower();
            this.DeveloperToken = DeveloperToken;
			this.Body = Body;
			this.EnableGzipCompression = EnableGzipCompression;

		}
		
		public string DownloadUrl { set; get; }
		public string ClientCustomerId { set; get; }
		public string ClientEmail { set; get; } // TODO : remove
		public string AuthToken { set; get; }
		public string ReturnMoneyInMicros { set; get; }
        public string DeveloperToken { set; get; }
		public string Body { set; get; }
		public string EnableGzipCompression { set; get; }
	}

	
}
