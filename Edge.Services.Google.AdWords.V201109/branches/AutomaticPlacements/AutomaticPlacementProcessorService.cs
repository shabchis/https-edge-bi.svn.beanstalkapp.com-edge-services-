using System;
using System.Collections.Generic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using GA = Google.Api.Ads.AdWords.v201109;

using System.IO;
using Edge.Services.GenericMetrics;

namespace Edge.Services.Google.AdWords
{
	class AutomaticPlacementProcessorService : PipelineService
	{
		static Dictionary<string, string> GoogleMeasuresDic;

		public AutomaticPlacementProcessorService()
		{
			GoogleMeasuresDic = new Dictionary<string, string>()
			{
				{Const.ConversionOnePerClick,"TotalConversionsOnePerClick"},
				{Const.ConversionManyPerClick,"TotalConversionsManyPerClick"}
			};
		}

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			string[] requiredHeaders = new string[] {Const.AutoPlacRequiredHeader};

			//Open Auto Plac file
			DeliveryFile _autoPlacFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT]];
			var _autoPlacReader = new CsvDynamicReader(_autoPlacFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);

			using (var session = new GenericMetricsImportManager(this.Instance.InstanceID)
			{
				
				MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
				MeasureOptionsOperator = OptionsOperator.Not,
				SegmentOptions = Data.Objects.SegmentOptions.All,
				SegmentOptionsOperator = OptionsOperator.And
			})
			{
				Dictionary<string, double> _totals = new Dictionary<string, double>();

				session.BeginImport(this.Delivery);

				//Intializing totals for validation
				foreach (KeyValuePair<string, Measure> measure in session.Measures)
				{
					if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
					{
						_totals.Add(measure.Key, 0);
					}

				}

				using (_autoPlacReader)
				{
					while (_autoPlacReader.Read())
					{

						#region Setting Totals
						/*==================================================================================================================*/
						// If end of file
						if (_autoPlacReader.Current[Const.CampaignIdFieldName] == Const.EOF)
						{
							//Setting totals for validation from totals line in adowrds file
							foreach (KeyValuePair<string, Measure> measure in session.Measures)
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
						
						autoPlacMetricsUnit.Channel = new Channel() { ID = 1 };
						autoPlacMetricsUnit.Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_autoPlacFile.Parameters["AdwordsClientID"] };

						autoPlacMetricsUnit.SegmentDimensions = new Dictionary<Segment, SegmentObject>();

						//ADDING CAMPAIGN
						Campaign campaign = new Campaign()
						{
							OriginalID = _autoPlacReader.Current[Const.CampaignIdFieldName],
							Name = _autoPlacReader.Current[Const.CampaignFieldName]
						};

						autoPlacMetricsUnit.SegmentDimensions.Add(session.SegmentTypes[Segment.Common.Campaign], campaign);

						//ADDING ADGROUP
						AdGroup adgroup = new AdGroup()
						{
							Campaign = campaign,
							Value = _autoPlacReader.Current[Const.AdGroupFieldName],
							OriginalID = _autoPlacReader.Current[Const.AdGroupIdFieldName]
						};
						autoPlacMetricsUnit.SegmentDimensions.Add(session.SegmentTypes[Segment.Common.AdGroup], adgroup);

						//INSERTING METRICS DATA
						autoPlacMetricsUnit.MeasureValues = new Dictionary<Measure, double>();
						
						autoPlacMetricsUnit.MeasureValues.Add(session.Measures[Measure.Common.Clicks], Convert.ToInt64(_autoPlacReader.Current.Clicks));
						autoPlacMetricsUnit.MeasureValues.Add(session.Measures[Measure.Common.Cost], (Convert.ToDouble(_autoPlacReader.Current.Cost)) / 1000000);
						autoPlacMetricsUnit.MeasureValues.Add(session.Measures[Measure.Common.Impressions], Convert.ToInt64(_autoPlacReader.Current.Impressions));
						autoPlacMetricsUnit.MeasureValues.Add(session.Measures[GoogleMeasuresDic[Const.ConversionOnePerClick]], Convert.ToDouble(_autoPlacReader.Current[Const.ConversionOnePerClick]));
						autoPlacMetricsUnit.MeasureValues.Add(session.Measures[GoogleMeasuresDic[Const.ConversionManyPerClick]], Convert.ToDouble(_autoPlacReader.Current[Const.ConversionManyPerClick]));

						//CREATING PLACEMENT
						autoPlacMetricsUnit.TargetDimensions = new List<Target>();
						PlacementTarget placement = new PlacementTarget()
						{
							Placement = _autoPlacReader.Current[Const.DomainFieldName],
							PlacementType = PlacementType.Automatic
						};
						autoPlacMetricsUnit.TargetDimensions.Add(placement);

						//SETTING TIME PERIOD
						autoPlacMetricsUnit.PeriodStart = this.Delivery.TargetPeriod.Start.ToDateTime();
						autoPlacMetricsUnit.PeriodEnd = this.Delivery.TargetPeriod.End.ToDateTime();

						session.ImportMetrics(autoPlacMetricsUnit);
					}
				}

				session.HistoryEntryParameters.Add(Edge.Data.Pipeline.Common.Importing.Consts.DeliveryHistoryParameters.ChecksumTotals, _totals);
				session.EndImport();
				
			}

			return Core.Services.ServiceOutcome.Success;
		}
	}
}


