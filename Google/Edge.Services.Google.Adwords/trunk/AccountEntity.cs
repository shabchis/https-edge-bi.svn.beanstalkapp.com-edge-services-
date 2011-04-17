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
        }
        public AccountEntity(int AccountID)
        {
            Id = AccountID;
        }
      
        public int Id { get; set; }
        public string User {get; set;}
        public string Password {get; set;}
        public List<string> Emails { get; set; } // To Do : split emails from configuration . 


        internal void GetAccountAccessInfo()
        {
            throw new NotImplementedException();
        }
    }
   

}
