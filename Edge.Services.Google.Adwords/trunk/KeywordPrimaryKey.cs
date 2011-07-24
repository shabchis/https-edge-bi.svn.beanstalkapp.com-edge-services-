using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace Edge.Services.Google.AdWords
{
	class KeywordPrimaryKey
	{
		public KeywordPrimaryKey()
		{

		}
		public override string ToString()
		{
			return string.Format(string.Format("{0}#{1}#{2}", AdgroupId, this.KeywordId, this.CampaignId));
		}

		public List<string> ToList()
		{
			return new List<string>()
			{
				AdgroupId.ToString(),
				KeywordId.ToString(),
				CampaignId.ToString()
			};
		}
		//public override int GetHashCode()
		//{
		//    return int.Parse(string.Format("{0}{1}{2}", AdgroupId.GetHashCode(), this.KeywordId.GetHashCode(), this.CampaignId.GetHashCode()));
		//    //return this.AdgroupId.GetHashCode() ^ this.KeywordId.GetHashCode() ^ this.CampaignId.GetHashCode();
		//}

		public long AdgroupId { set; get; }
		public long KeywordId { set; get; }
		public long CampaignId { set; get; }

	}
}
