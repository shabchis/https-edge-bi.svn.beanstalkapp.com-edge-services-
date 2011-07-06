using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Google.Adwords
{
    class AccountEntity
    {
        public AccountEntity()
        {
			Emails = new List<string>();
        }
        public AccountEntity(int AccountID, string EmailsString)
        {
            Id = AccountID;
			Emails = new List<string>();
			foreach (string mail in EmailsString.Split('|').ToList<string>())
			{
				Emails.Add(mail);
			}
        }
      
        public int Id { get; set; }
        public List<string> Emails { get; set; }

		


        internal void GetAccountAccessInfo()
        {
            throw new NotImplementedException();
        }
    }
   

}
