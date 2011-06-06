using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.Google.Adwords
{
	class KeywordPrimaryKey
	{
		public KeywordPrimaryKey()
		{

		}

		public override int GetHashCode()
		{
			return this.AdgroupId.GetHashCode() ^ this.KeywordId.GetHashCode() ^ this.CampaignId.GetHashCode();
		}

		public long AdgroupId { set; get; }
		public long KeywordId { set; get; }
		public long CampaignId { set; get; }
	}
}
