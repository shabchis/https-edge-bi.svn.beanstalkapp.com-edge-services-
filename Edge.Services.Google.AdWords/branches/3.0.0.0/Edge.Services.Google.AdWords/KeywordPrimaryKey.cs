using System.Collections.Generic;

namespace Edge.Services.Google.AdWords
{
	class KeywordPrimaryKey
	{
		public override string ToString()
		{
			return string.Format("{0}#{1}#{2}", AdgroupId, KeywordId, CampaignId);
		}

		public List<string> ToList()
		{
			return new List<string>
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
