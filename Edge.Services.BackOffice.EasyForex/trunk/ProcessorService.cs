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
using System.Xml;
using System.Runtime.Serialization.Formatters;

namespace Edge.Services.BackOffice.EasyForex
{
	public class ProcessorService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			//INIT 
			this.Delivery = this.NewDelivery();
			this.Delivery.Account = new Account() { ID = this.Instance.AccountID };
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Channel() { ID = -1 };
			this.Delivery.TargetLocationDirectory = @"D:\";
			this.Delivery.Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]", this.Instance.AccountID, this.TargetPeriod.ToAbsolute()));

			this.Delivery.Save();
			StreamReader streamReader = new StreamReader(@"D:\test\20120102-b52e15408f514c6d9965c2c62aebe566-EasyForexBackOffice.xml", Encoding.UTF8);
			// ------------------------------------------
			// ------------------------------------------
			// ------------------------------------------
			var boReader = new XmlDynamicReader(streamReader.BaseStream,
				"Envelope/Body/GetGatewayStatisticsResponse/GetGatewayStatisticsResult/diffgram/DSMarketing/CampaignStatisticsForEasyNet");

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

						boMetricsUnit.MeasureValues[session.Measures["Scoring"]] = Convert.ToDouble(boReader.Current.Attributes.Scoring);
						boMetricsUnit.MeasureValues[session.Measures["TotalHits"]] = Convert.ToDouble(boReader.Current.Attributes.TotalHits);
						boMetricsUnit.MeasureValues[session.Measures["NewLeads"]] = Convert.ToDouble(boReader.Current.Attributes.NewLeads);
						boMetricsUnit.MeasureValues[session.Measures["Acquisition1"]] = Convert.ToDouble(boReader.Current.Attributes.NewUsers);
						boMetricsUnit.MeasureValues[session.Measures["Acquisition2"]] = Convert.ToDouble(boReader.Current.Attributes.NewActiveUsers);
						boMetricsUnit.MeasureValues[session.Measures["NewDeposit"]] = Convert.ToDouble(boReader.Current.Attributes.NewDeposit);
						boMetricsUnit.MeasureValues[session.Measures["ActiveUsers"]] = Convert.ToDouble(boReader.Current.Attributes.ActiveUsers);
						boMetricsUnit.MeasureValues[session.Measures["TotalNewDeposit"]] = Convert.ToDouble(boReader.Current.Attributes.TotalNewDeposit);
						boMetricsUnit.MeasureValues[session.Measures["SAT"]] = Convert.ToDouble(boReader.Current.Attributes.SAT);
						boMetricsUnit.MeasureValues[session.Measures["TotalEV"]] = Convert.ToDouble(boReader.Current.Attributes.TotalEV);

						// Add segment values
						boMetricsUnit.Segments[Segment.TrackerSegment] = new SegmentValue() { Value = boReader.Current.GID };
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
