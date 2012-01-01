using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Services.SegmentMetrics;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Newtonsoft.Json;
using System.IO;

namespace Edge.Services.BackOffice.EasyForex
{
	public class ProcessorService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			// ------------------------------------------
			// ------------------------------------------
			// ------------------------------------------
			// THIS IS JUST FOR SETTING UP EXAMPLE

			//INIT 
			this.Delivery = this.NewDelivery();
			this.Delivery.Account = new Account() { ID = this.Instance.AccountID };
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Channel() { ID = -1 };
			this.Delivery.TargetLocationDirectory = @"D:\";
			this.Delivery.Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]",this.Instance.AccountID,this.TargetPeriod.ToAbsolute()));

			this.Delivery.Save();

			StreamReader streamReader = new StreamReader(@"D:\test\ 007 20111229@0440 (1026240) .xml", Encoding.UTF8);
			// ------------------------------------------
			// ------------------------------------------
			// ------------------------------------------

			var boReader = new XmlDynamicReader(streamReader.BaseStream, "DocumentElement/CampaignStatisticsForEasyNetSeperia");

			using (var session = new SegmentMetricsImportManager(this.Instance.InstanceID))
			{
				session.BeginImport(this.Delivery);

				using (boReader)
				{
					while (boReader.Read())
					{
						SegmentMetricsUnit boMetricsUnit = new SegmentMetricsUnit();

						// Add measure values
						

						double gid = Convert.ToDouble(boReader.Current.GID);

						boMetricsUnit.MeasureValues[session.Measures["leads"]] = Convert.ToDouble(boReader.Current.Attributes.leads);
						//boMetricsUnit.MeasureValues[session.Measures["registrations"]] = boReader.Current.registrations;
						//boMetricsUnit.MeasureValues[session.Measures["uniquepayingclients"]] = boReader.Current.uniquepayingclients;
						//boMetricsUnit.MeasureValues[session.Measures["sales"]] = boReader.Current.sales;
						//boMetricsUnit.MeasureValues[session.Measures["refunds"]] = boReader.Current.refunds;


						// Add segment values
						boMetricsUnit.Segments[Segment.TrackerSegment] = new SegmentValue() { Value = boReader.Current.Attributes.id };
						//boMetricsUnit.Segments[session.Segments["Affiliate"]] = boReader.Current.affiliate;

						//Create Usid
						Dictionary<string, string> usid = new Dictionary<string, string>();
						foreach (var segment in boMetricsUnit.Segments)
						{
							usid.Add(segment.Key.Name, segment.Value.Value);
						}

						boMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
						boMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

						session.ImportMetrics(boMetricsUnit,JsonConvert.SerializeObject(usid));
					}
				}

				session.EndImport();

			}
			return Core.Services.ServiceOutcome.Success;
		}
	}
	
}
