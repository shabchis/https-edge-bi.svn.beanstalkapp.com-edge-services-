using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Core.Services;
using Edge.Core.Utilities;
using Edge.Data.Objects;
using Edge.Data.Objects.Reflection;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Common.Importing;


namespace Edge.Services.AdMetrics
{
	/// <summary>
	/// Encapsulates the process of adding ads and ad metrics to the delivery staging database.
	/// </summary>
	public class AdMetricsImportManager : MetricsImportManager<AdMetricsUnit>
	{
		#region Table structure
		/*=========================*/

		private static class Tables
		{
			public class Ad
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef Channel_ID = new ColumnDef("Channel_ID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef Account_ID = new ColumnDef("Account_ID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef Account_OriginalID = new ColumnDef("Account_OriginalID", type: SqlDbType.NVarChar, size: 100, nullable: true);
				public static ColumnDef Name = new ColumnDef("Name", size: 100);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef AdStatus = new ColumnDef("AdStatus", type: SqlDbType.Int, nullable: true);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public class AdCreative
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef Name = new ColumnDef("Name", size: 100);
				public static ColumnDef CreativeType = new ColumnDef("CreativeType", type: SqlDbType.Int);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public class AdTarget
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef TargetType = new ColumnDef("TargetType", type: SqlDbType.Int);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public class AdSegment
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef SegmentType = new ColumnDef("SegmentType", type: SqlDbType.Int, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 4000);
				public static ColumnDef Value = new ColumnDef("Value", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public class Metrics
			{
				public static ColumnDef MetricsUsid = new ColumnDef("MetricsUsid", size: 32, type: SqlDbType.Char, nullable: false);
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef DownloadedDate = new ColumnDef("DownloadedDate", type: SqlDbType.DateTime, nullable: true, defaultValue: "GetDate()");
				public static ColumnDef TargetPeriodStart = new ColumnDef("TargetPeriodStart", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef TargetPeriodEnd = new ColumnDef("TargetPeriodEnd", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef Currency = new ColumnDef("Currency", size: 10);
			}

			public class MetricsDimensionTarget
			{
				public static ColumnDef MetricsUsid = new ColumnDef("MetricsUsid", size: 32, type: SqlDbType.Char, nullable: false);
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef TargetType = new ColumnDef("TargetType", type: SqlDbType.Int);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}
		}

		/*=========================*/
		#endregion


		public AdMetricsImportManager(long serviceInstanceID, ImportManagerOptions options = null)
			: base(serviceInstanceID, options)
		{
		}

		public Func<Ad, long> OnAdUsidRequired = null;
		private string GetAdIdentity(Ad ad)
		{
			string val;
			if (this.OnAdUsidRequired != null)
				val = this.OnAdUsidRequired(ad).ToString();
			else if (String.IsNullOrEmpty(ad.OriginalID))
				throw new Exception("Ad.OriginalID is required. If it is not available, provide a function for AdMetricsImportManager.OnAdUsidRequired that returns a unique value for this ad.");
			else
				val = ad.OriginalID.ToString();

			return val;
		}


		public void ImportAd(Ad ad)
		{
			EnsureBeginImport();

			string adUsid = GetAdIdentity(ad);

			// Ad
			var adRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Ad.AdUsid, adUsid},
				{Tables.Ad.Channel_ID, ad.Channel == null ? -1 : ad.Channel.ID },
				{Tables.Ad.Account_ID, ad.Account == null ? -1 : ad.Account.ID },
				{Tables.Ad.Account_OriginalID, ad.Account.OriginalID},
				{Tables.Ad.Name, ad.Name},
				{Tables.Ad.OriginalID, ad.OriginalID},
				{Tables.Ad.DestinationUrl, ad.DestinationUrl},
				{Tables.Ad.AdStatus, ad.Status}
			};
			foreach (KeyValuePair<ExtraField, object> extraField in ad.ExtraFields)
				adRow[new ColumnDef(Tables.AdTarget.ExtraFieldX, extraField.Key.ColumnIndex)] = extraField.Value;

			Bulk<Tables.Ad>().SubmitRow(adRow);

			// AdTarget
			foreach (Target target in ad.Targets)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.AdTarget.AdUsid, adUsid },
					{ Tables.AdTarget.OriginalID, target.OriginalID },
					{ Tables.AdTarget.DestinationUrl, target.DestinationUrl },
					{ Tables.AdTarget.TargetType, target.TypeID }
				};

				foreach (KeyValuePair<MappedObjectField, object> fixedField in target.GetFieldValues())
					row[new ColumnDef(Tables.AdTarget.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> extraField in target.ExtraFields)
					row[new ColumnDef(Tables.AdTarget.ExtraFieldX, extraField.Key.ColumnIndex)] = extraField.Value;

				Bulk<Tables.AdTarget>().SubmitRow(row);
			}

			// AdCreative
			foreach (Creative creative in ad.Creatives)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.AdCreative.AdUsid, adUsid },
					{ Tables.AdCreative.OriginalID, creative.OriginalID },
					{ Tables.AdCreative.Name, creative.Name },
					{ Tables.AdCreative.CreativeType, creative.TypeID }
				};

				foreach (KeyValuePair<MappedObjectField, object> fixedField in creative.GetFieldValues())
					row[new ColumnDef(Tables.AdCreative.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> extraField in creative.ExtraFields)
					row[new ColumnDef(Tables.AdCreative.ExtraFieldX, extraField.Key.ColumnIndex)] = extraField.Value;

				Bulk<Tables.AdCreative>().SubmitRow(row);
			}

			// AdSegment
			foreach (Segment segment in ad.Segments)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.AdSegment.AdUsid, adUsid },
					{ Tables.AdSegment.SegmentType, segment.TypeID },
					{ Tables.AdSegment.OriginalID, segment.OriginalID },
					{ Tables.AdSegment.Value, segment.Value }
					
				};

				foreach (KeyValuePair<MappedObjectField, object> fixedField in segment.GetFieldValues())
					row[new ColumnDef(Tables.AdSegment.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> extraField in segment.ExtraFields)
					row[new ColumnDef(Tables.AdSegment.ExtraFieldX, extraField.Key.ColumnIndex)] = extraField.Value;

				Bulk<Tables.AdSegment>().SubmitRow(row);
			}
		}


		public void ImportMetrics(AdMetricsUnit metrics)
		{
			EnsureBeginImport();

			if (metrics.Ad == null)
				throw new InvalidOperationException("Cannot import a metrics unit that is not associated with an ad.");

			string adUsid = GetAdIdentity(metrics.Ad);
			string metricsUsid = metrics.Usid.ToString("N");

			// Metrics
			var metricsRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Metrics.MetricsUsid, metricsUsid},
				{Tables.Metrics.AdUsid, adUsid},
				{Tables.Metrics.TargetPeriodStart, metrics.PeriodStart},
				{Tables.Metrics.TargetPeriodEnd, metrics.PeriodEnd},
				{Tables.Metrics.Currency, metrics.Currency == null ? null : metrics.Currency.Code}
			};

			foreach (KeyValuePair<Measure, double> measure in metrics.MeasureValues)
				metricsRow[new ColumnDef(measure.Key.Name)] = measure.Value;

			Bulk<Tables.Metrics>().SubmitRow(metricsRow);

			// MetricsDimensionTarget
			foreach (Target target in metrics.TargetDimensions)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.MetricsDimensionTarget.MetricsUsid, metricsUsid },
					{ Tables.MetricsDimensionTarget.AdUsid, adUsid },
					{ Tables.MetricsDimensionTarget.OriginalID, target.OriginalID },
					{ Tables.MetricsDimensionTarget.TargetType, target.TypeID },
					{ Tables.MetricsDimensionTarget.DestinationUrl, target.DestinationUrl }
				};

				foreach (KeyValuePair<MappedObjectField, object> fixedField in target.GetFieldValues())
					row[new ColumnDef(Tables.MetricsDimensionTarget.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> customField in target.ExtraFields)
					row[new ColumnDef(Tables.MetricsDimensionTarget.ExtraFieldX, customField.Key.ColumnIndex)] = customField.Value;

				Bulk<Tables.MetricsDimensionTarget>().SubmitRow(row);
			}

		}

		protected override string TablePrefixType
		{
			get { return "AD"; }
		}

		protected override MeasureOptions MeasureOptions
		{
			get { return MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice; }
		}

		protected override OptionsOperator MeasureOptionsOperator
		{
			get { return OptionsOperator.Not; }
		}

		//protected override SegmentOptions SegmentOptions
		//{
		//    get { throw new NotImplementedException(); }
		//}

		//protected override OptionsOperator SegmentOptionsOperator
		//{
		//    get { throw new NotImplementedException(); }
		//}

		protected override Type MetricsTableDefinition
		{
			get { return typeof(Tables.Metrics); }
		}
	}
}
