using System;
using System.Collections.Generic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using GA = Google.Api.Ads.AdWords.v201309;
using Edge.Data.Pipeline.Metrics;
using System.Linq;
using System.IO;
using Edge.Data.Pipeline.Metrics.GenericMetrics;
using Edge.Data.Pipeline.Common.Importing;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Metrics.AdMetrics;

namespace Edge.Services.Google.AdWords
{
	
	class AutomaticPlacementProcessorService : MetricsProcessorServiceBase
	{
		static Dictionary<string, string> GoogleMeasuresDic;
		static Dictionary<string, ObjectStatus> ObjectStatusDic;

		public AutomaticPlacementProcessorService()
		{
			GoogleMeasuresDic = new Dictionary<string, string>()
			{
				{Const.ConversionOnePerClickFieldName,"TotalConversionsOnePerClick"},
				{Const.ConversionManyPerClickFieldName,"TotalConversionsManyPerClick"}
			};

			ObjectStatusDic = new Dictionary<string, ObjectStatus>()
			{
				{"PAUSED",ObjectStatus.Paused},
				{"DELETED",ObjectStatus.Deleted},
				{"ACTIVE",ObjectStatus.Active}
			};
		}

		public new GenericMetricsImportManager ImportManager
		{
			get { return (GenericMetricsImportManager)base.ImportManager; }
			set { base.ImportManager = value; }
		}

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			string[] requiredHeaders = new string[] {Const.AutoPlacRequiredHeader};
			DeliveryOutput currentOutput = Delivery.Outputs.First();

			//Open Auto Plac file
			DeliveryFile _autoPlacFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT]];
			var _autoPlacReader = new CsvDynamicReader(_autoPlacFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);

			using (this.ImportManager = new GenericMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
			{
				MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
				MeasureOptionsOperator = OptionsOperator.Not,
				SegmentOptions = Data.Objects.SegmentOptions.All,
				SegmentOptionsOperator = OptionsOperator.And
			}))
			{
				Dictionary<string, double> _totals = new Dictionary<string, double>();

				this.ImportManager.BeginImport(this.Delivery);

				//Intializing totals for validation
				foreach (KeyValuePair<string, Measure> measure in this.ImportManager.Measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						_totals.Add(measure.Key, 0);
					}

				}

				using (_autoPlacReader)
				{
					this.Mappings.OnFieldRequired = field => _autoPlacReader.Current[field];

					while (_autoPlacReader.Read())
					{
						#region Setting Totals
						/*==================================================================================================================*/
						// If end of file
						if (_autoPlacReader.Current[Const.CampaignIdFieldName] == Const.EOF)
						{
							//Setting totals for validation from totals line in adowrds file
							foreach (KeyValuePair<string, Measure> measure in this.ImportManager.Measures)
							{
								if (!measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
									continue;

								switch (measure.Key)
								{
									case Measure.Common.Clicks: _totals[Measure.Common.Clicks] = Convert.ToInt64(_autoPlacReader.Current.Clicks); break;
									case Measure.Common.Cost: _totals[Measure.Common.Cost] = (Convert.ToDouble(_autoPlacReader.Current.Cost)) / 1000000; break;
									case Measure.Common.Impressions: _totals[Measure.Common.Impressions] = Convert.ToInt64(_autoPlacReader.Current.Impressions); break;
								}
							}
							break;
						}
						/*==================================================================================================================*/
						#endregion

						GenericMetricsUnit autoPlacMetricsUnit = new GenericMetricsUnit();
						autoPlacMetricsUnit.Output = currentOutput;
						
						autoPlacMetricsUnit.Channel = new Channel() { ID = 1 };
						autoPlacMetricsUnit.Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_autoPlacFile.Parameters["AdwordsClientID"] };

						autoPlacMetricsUnit.SegmentDimensions = new Dictionary<Segment, SegmentObject>();

						//ADDING CAMPAIGN
						Campaign campaign = new Campaign()
						{
							OriginalID = _autoPlacReader.Current[Const.CampaignIdFieldName],
							Name = _autoPlacReader.Current[Const.CampaignFieldName],
							Status = ObjectStatusDic[((string)_autoPlacReader.Current[Const.CampaignStatusFieldName]).ToUpper()]
						};

						autoPlacMetricsUnit.SegmentDimensions.Add(this.ImportManager.SegmentTypes[Segment.Common.Campaign], campaign);

						//ADDING ADGROUP
						AdGroup adgroup = new AdGroup()
						{
							Campaign = campaign,
							Value = _autoPlacReader.Current[Const.AdGroupFieldName],
							OriginalID = _autoPlacReader.Current[Const.AdGroupIdFieldName]
						};
						autoPlacMetricsUnit.SegmentDimensions.Add(this.ImportManager.SegmentTypes[Segment.Common.AdGroup], adgroup);

						//INSERTING METRICS DATA
						autoPlacMetricsUnit.MeasureValues = new Dictionary<Measure, double>();
						
						autoPlacMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Clicks], Convert.ToInt64(_autoPlacReader.Current.Clicks));
						autoPlacMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Cost], (Convert.ToDouble(_autoPlacReader.Current.Cost)) / 1000000);
						autoPlacMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Impressions], Convert.ToInt64(_autoPlacReader.Current.Impressions));
						autoPlacMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionOnePerClickFieldName]], Convert.ToDouble(_autoPlacReader.Current[Const.ConversionOnePerClickFieldName]));
						autoPlacMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionManyPerClickFieldName]], Convert.ToDouble(_autoPlacReader.Current[Const.ConversionManyPerClickFieldName]));

						//CREATING PLACEMENT
						autoPlacMetricsUnit.TargetDimensions = new List<Target>();
						PlacementTarget placement = new PlacementTarget()
						{
							Placement = _autoPlacReader.Current[Const.DomainFieldName],
							PlacementType = PlacementType.Automatic
							// Add status !!!
						};
						autoPlacMetricsUnit.TargetDimensions.Add(placement);

						//SETTING TIME PERIOD
						autoPlacMetricsUnit.TimePeriodStart = this.Delivery.TimePeriodDefinition.Start.ToDateTime();
						autoPlacMetricsUnit.TimePeriodEnd = this.Delivery.TimePeriodDefinition.End.ToDateTime();

						this.ImportManager.ImportMetrics(autoPlacMetricsUnit);
					}
				}

				this.Delivery.Outputs.First().Checksum = _totals;
				this.ImportManager.EndImport();
				
			}

			return Core.Services.ServiceOutcome.Success;
		}
	}
}


