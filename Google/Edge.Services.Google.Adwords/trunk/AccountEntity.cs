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
        public AccountEntity(int AccountID)
        {
            Id = AccountID;
			Emails = new List<string>();
        }
      
        public int Id { get; set; }
        public List<string> Emails { get; set; } // To Do : split emails from configuration.

		


        internal void GetAccountAccessInfo()
        {
            throw new NotImplementedException();
        }
    }
   

}
