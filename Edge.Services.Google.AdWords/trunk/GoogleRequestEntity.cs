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
        public GoogleRequestEntity(Uri Url, string Id, string Email, string AuthToken, bool IsReturnMoneyInMicros, string DeveloperToken)
		{
			this.downloadUrl = Url;
            this.clientCustomerId = Id;
            this.clientEmail = Email;
            this.authToken = AuthToken;
            this.returnMoneyInMicros = IsReturnMoneyInMicros.ToString().ToLower();
            this.developerToken = DeveloperToken;
		}
		
		public Uri downloadUrl { set; get; }
		public string clientCustomerId { set; get; }
		public string clientEmail { set; get; } // TODO : remove
		public string authToken { set; get; }
		public string returnMoneyInMicros { set; get; }
        public string developerToken { set; get; }
	}

	
}
