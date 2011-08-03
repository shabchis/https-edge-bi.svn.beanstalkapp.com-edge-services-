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
		public GoogleRequestEntity(Uri Url , string Id , string Email , string AuthToken , bool IsReturnMoneyInMicros)
		{
			downloadUrl = Url;
			clientCustomerId = Id;
			clientEmail = Email;
			authToken = AuthToken;
			returnMoneyInMicros = IsReturnMoneyInMicros.ToString().ToLower();
		}
		
		public Uri downloadUrl { set; get; }
		public string clientCustomerId { set; get; }
		public string clientEmail { set; get; } // TODO : remove
		public string authToken { set; get; }
		public string returnMoneyInMicros { set; get; }
	}

	
}
