using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;

namespace Edge.Services.Facebook.AdsApi
{
	public class InitializerService: PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			//TODO: TALK WITH DORON I THINK ITS IS PARENTINSTANCEID
			// Create a new delivery
			this.Delivery = new Delivery(this.Instance.InstanceID)
			{
				TargetPeriod = this.TargetPeriod
			};

			//set parameters for entire delivery
			this.Delivery.Parameters["AccountID"] = this.Instance.AccountID;

			if (this.Instance.Configuration.Options["APIKey"] == null)
				this.Delivery.Parameters["APIKey"] = this.Instance.ParentInstance.Configuration.Options["APIKey"].ToString();
			else
				this.Delivery.Parameters["APIKey"] = this.Instance.Configuration.Options["APIKey"].ToString();


			if (this.Instance.Configuration.Options["sessionKey"] == null)
				this.Delivery.Parameters["sessionKey"] = this.Instance.ParentInstance.Configuration.Options["sessionKey"].ToString();
			else
				this.Delivery.Parameters["sessionKey"] = Instance.Configuration.Options["sessionKey"].ToString();

			if (this.Instance.Configuration.Options["applicationSecret"] == null)
				this.Delivery.Parameters["applicationSecret"] = this.Instance.ParentInstance.Configuration.Options["applicationSecret"].ToString();
			else
				this.Delivery.Parameters["applicationSecret"] = this.Instance.Configuration.Options["applicationSecret"].ToString();

			if (Instance.Configuration.Options["FBaccountID"] == null)
				this.Delivery.Parameters["FBaccountID"] = this.Instance.ParentInstance.Configuration.Options["FBaccountID"].ToString();
			else
				this.Delivery.Parameters["FBaccountID"] = this.Instance.Configuration.Options["FBaccountID"].ToString();


			if (Instance.Configuration.Options["accountName"] == null)
				this.Delivery.Parameters["accountName"] = this.Instance.ParentInstance.Configuration.Options["accountName"].ToString();
			else
				this.Delivery.Parameters["accountName"] = this.Instance.Configuration.Options["accountName"].ToString();

			if (Instance.Configuration.Options["sessionSecret"] == null)
				this.Delivery.Parameters["sessionSecret"] = this.Instance.ParentInstance.Configuration.Options["sessionSecret"].ToString();
			else
				this.Delivery.Parameters["sessionSecret"] = this.Instance.Configuration.Options["sessionSecret"].ToString();
			this.ReportProgress(0.2);

			DeliveryFile deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroupStats";
			deliveryFile.Parameters.Add("AdGroupStatsParameters", GetAdGroupStatsParameters());
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.4);
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroupCreatives";
			deliveryFile.Parameters.Add("AdGroupCreativesParameters", GetAdGroupCreativesParameters());
			this.Delivery.Files.Add(deliveryFile);
			this.ReportProgress(0.6);

			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetAdGroups";
			deliveryFile.Parameters.Add("AdGroupsParameters", GetAdGroupsParameters());
			this.Delivery.Files.Add(deliveryFile);

			this.ReportProgress(0.8);
			deliveryFile = new DeliveryFile();
			deliveryFile.Name = "GetCampaigns";
			deliveryFile.Parameters.Add("CampaignsParmaters", GetCampaignsParmaters());
			this.Delivery.Files.Add(deliveryFile);
			this.ReportProgress(0.98);
			this.Delivery.Save();
			this.ReportProgress(0.99);
			return ServiceOutcome.Success;
		}

		private Dictionary<string, string> GetAdGroupStatsParameters()
		{
			Dictionary<string, string> AdGroupStatesParameters = new Dictionary<string, string>();
			AdGroupStatesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupStatesParameters.Add("method", "facebook.ads.getAdGroupStats");			
			AdGroupStatesParameters.Add("campaign_ids", "");
			AdGroupStatesParameters.Add("adgroup_ids", "");
			AdGroupStatesParameters.Add("include_deleted","false");

			//TODO: TALK WITH Doron and amit about timerange + 10 hours diffreance
			//TODO: CHANGE TO "TODATETIME"
			string timeRange = string.Format("\"time_ranges\": { \"day_start\":{ :{\"month\":{0},\"day\":{1},\"year\":{2}},\"day_stop\":{\"month\":{3},\"day\":{4},\"year\":{5}}}}", TargetPeriod.Start.t.Month, TargetPeriod.Start.Day, TargetPeriod.Start.Year, TargetPeriod.End.Month, TargetPeriod.End.Day, TargetPeriod.End.Year);
												
												

			return AdGroupStatesParameters;
		}
		private Dictionary<string, string> GetAdGroupCreativesParameters()
		{
			Dictionary<string, string> AdGroupCreativesParameters = new Dictionary<string, string>();
			AdGroupCreativesParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupCreativesParameters.Add("method", "facebook.ads.getAdGroupCreatives");
			AdGroupCreativesParameters.Add("include_deleted", "false");
			//TODO:  for API bug 2011-03-21 TALK WITH DORON MAX CAMPAIGNS PER ARRAY
			AdGroupCreativesParameters.Add("campaign_ids", "");		
			AdGroupCreativesParameters.Add("adgroup_ids", "");

			return AdGroupCreativesParameters;
		}
		private Dictionary<string, string> GetAdGroupsParameters()
		{
			Dictionary<string, string> AdGroupsParameters = new Dictionary<string, string>();
			AdGroupsParameters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			AdGroupsParameters.Add("method", "facebook.ads.getAdGroups");
			AdGroupsParameters.Add("include_deleted", "false");
			AdGroupsParameters.Add("campaign_ids", "");
			AdGroupsParameters.Add("adgroup_ids", "");

			return AdGroupsParameters;
		}
		private Dictionary<string, string> GetCampaignsParmaters()
		{
			Dictionary<string, string> CampaignsParmaters = new Dictionary<string, string>();
			CampaignsParmaters.Add("account_id", this.Delivery.Parameters["FBaccountID"].ToString());
			CampaignsParmaters.Add("method", "facebook.ads.getCampaigns");
			CampaignsParmaters.Add("include_deleted", "false");
			CampaignsParmaters.Add("campaign_ids", "");

			return CampaignsParmaters;



		}



		
	}
}
