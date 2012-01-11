﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Core.Data;

namespace Edge.Services.Metrics.Validations
{
	abstract public class DeliveryDBChecksumBaseService : ValidationService
	{
		abstract protected ValidationResult DeliveryDbCompare(Delivery delivery, Dictionary<string, double> totals, string DbConnectionStringName, string comparisonTable);
		public static Double ALLOWED_DIFF = 0.1;
		public double progress = 0;

		protected override IEnumerable<ValidationResult> Validate()
		{
			Channel channel = new Channel();
			progress += 0.1;
			this.ReportProgress(progress);

			#region Getting Service option params
			//Getting Accounts list
			string[] accounts;
			if (this.Instance.AccountID == -1)
			{
				if (String.IsNullOrEmpty(this.Instance.Configuration.Options["AccountsList"]))
					throw new Exception("Missing Configuration option AccountsList");
				accounts = this.Instance.Configuration.Options["AccountsList"].Split(',');
			}
			else
			{
				List<string> account = new List<string>(){this.Instance.AccountID.ToString()};
				accounts = account.ToArray();
			}


			//Getting Table 
			string comparisonTable;
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["SourceTable"]))
				throw new Exception("Missing Configuration option SourceTable");
			else comparisonTable = this.Instance.Configuration.Options["SourceTable"];

			//Getting Channel List
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["ChannelList"]))
				throw new Exception("Missing Configuration option ChannelList");
			string[] channels = this.Instance.Configuration.Options["ChannelList"].Split(',');

			//Getting TimePeriod
			DateTime fromDate, toDate;
			if ((String.IsNullOrEmpty(this.Instance.Configuration.Options["fromDate"])) && (String.IsNullOrEmpty(this.Instance.Configuration.Options["toDate"])))
			{
				fromDate = this.TargetPeriod.Start.ToDateTime();
				toDate = this.TargetPeriod.End.ToDateTime();
			}
			else
			{
				fromDate = Convert.ToDateTime(this.Instance.Configuration.Options["fromDate"]);
				toDate = Convert.ToDateTime(this.Instance.Configuration.Options["toDate"]);
			}
			#endregion



			if (this.Delivery == null || this.Delivery.DeliveryID.Equals(Guid.Empty))
			{
				#region Creating Delivery Search List
				List<DeliverySearchItem> deliverySearchList = new List<DeliverySearchItem>();

				while (fromDate <= toDate)
				{
					// {start: {base : '2009-01-01', h:0}, end: {base: '2009-01-01', h:'*'}}
					var subRange = new DateTimeRange()
					{
						Start = new DateTimeSpecification()
						{
							BaseDateTime = fromDate,
							Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Exact, Value = 0 },
						},

						End = new DateTimeSpecification()
						{
							BaseDateTime = fromDate,
							Hour = new DateTimeTransformation() { Type = DateTimeTransformationType.Max },
						}
					};

					foreach (var Channel in channels)
					{

						foreach (string account in accounts)
						{
							DeliverySearchItem delivery = new DeliverySearchItem();
							delivery.account = new Account() { ID = Convert.ToInt32(account) };
							delivery.channel = new Channel() { ID = Convert.ToInt32(Channel) };
							delivery.targetPeriod = subRange;
							deliverySearchList.Add(delivery);

							progress += 0.3 * ((1 - progress) / (channels.LongLength + accounts.LongLength));
							this.ReportProgress(progress);
						}
					}
					fromDate = fromDate.AddDays(1);
				}
				#endregion
				foreach (DeliverySearchItem deliveryToSearch in deliverySearchList)
				{
					#region Foreach

					//Getting criterion matched deliveries
					Delivery[] deliveriesToCheck = Delivery.GetByTargetPeriod(deliveryToSearch.targetPeriod.Start.ToDateTime(), deliveryToSearch.targetPeriod.End.ToDateTime(), deliveryToSearch.channel, deliveryToSearch.account);
					bool foundCommited = false;

					progress += 0.3 * (1 - progress);
					this.ReportProgress(progress);

					foreach (Delivery d in deliveriesToCheck)
					{
						int rollbackIndex = -1;
						int commitIndex = -1;

						#region Searching and researching commited and rolledback deliveries
						for (int i = 0; i < d.History.Count; i++)
						{
							if (d.History[i].Operation == DeliveryOperation.Committed)
								commitIndex = i;
							else if (d.History[i].Operation == DeliveryOperation.RolledBack)
								rollbackIndex = i;
						}

						if (commitIndex > rollbackIndex)
						{
							object totalso;
							foundCommited = true;

							DeliveryHistoryEntry commitEntry = null;
							IEnumerable<DeliveryHistoryEntry> processedEntries = d.History.Where(entry => (entry.Operation == DeliveryOperation.Imported));
							if (processedEntries != null && processedEntries.Count() > 0)
								commitEntry = (DeliveryHistoryEntry)processedEntries.Last();
							else
								continue;

							if (commitEntry.Parameters.TryGetValue(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, out totalso))
							{
								Dictionary<string, double> totals = (Dictionary<string, double>)totalso;

								//Check Delivery data vs OLTP
								yield return (DeliveryDbCompare(d, totals, "OltpDB", comparisonTable));
							}

						}
						#endregion

					}


					//could not find deliveries by user criterions
					if (deliveriesToCheck.Length == 0)
					{
						yield return new ValidationResult()
						{
							ResultType = ValidationResultType.Error,
							AccountID = deliveryToSearch.account.ID,
							TargetPeriodStart = deliveryToSearch.targetPeriod.Start.ToDateTime(),
							TargetPeriodEnd = deliveryToSearch.targetPeriod.End.ToDateTime(),
							Message = "Cannot find deliveries in DB",
							ChannelID = deliveryToSearch.channel.ID,
							CheckType = this.Instance.Configuration.Name
						};
					}
					else if (!foundCommited)
					{
						yield return new ValidationResult()
						{
							ResultType = ValidationResultType.Error,
							AccountID = deliveryToSearch.account.ID,
							TargetPeriodStart = deliveryToSearch.targetPeriod.Start.ToDateTime(),
							TargetPeriodEnd = deliveryToSearch.targetPeriod.End.ToDateTime(),
							Message = "Cannot find Commited deliveries in DB",
							ChannelID = deliveryToSearch.channel.ID,
							CheckType = this.Instance.Configuration.Name
						};
					}
					#endregion
				} // End of foreach

			}
			else
			{
				//Getting current Delivery totals
				object totalso;
				DeliveryHistoryEntry commitEntry = null;
				IEnumerable<DeliveryHistoryEntry> processedEntries = this.Delivery.History.Where(entry => (entry.Operation == DeliveryOperation.Imported));
				if (processedEntries != null && processedEntries.Count() > 0)
				{
					commitEntry = (DeliveryHistoryEntry)processedEntries.Last();
					if (commitEntry.Parameters.TryGetValue(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, out totalso))
					{
						Dictionary<string, double> totals = (Dictionary<string, double>)totalso;
						yield return (DeliveryDbCompare(this.Delivery, totals, "OltpDB", comparisonTable));
					}
				}
			}


		}

	}
	public class DeliverySearchItem
	{
		public Account account { set; get; }
		public Channel channel { set; get; }
		public DateTimeRange targetPeriod { set; get; }
	}
}
