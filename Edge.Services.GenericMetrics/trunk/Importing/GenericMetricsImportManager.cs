using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using Edge.Core.Configuration;
using Edge.Core.Data;
using Edge.Core.Utilities;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Common.Importing;
using Edge.Data.Objects.Reflection;

namespace Edge.Services.GenericMetrics
{
	public class GenericMetricsImportManager : MetricsImportManager<GenericMetricsUnit>
	{
		#region Table structure
		/*=========================*/
		public static class Tables
		{
			public class Metrics
			{
				public static ColumnDef MetricsUsid = new ColumnDef("MetricsUsid", size: 32, type: SqlDbType.Char, nullable: false);
				public static ColumnDef Channel_ID = new ColumnDef("Channel_ID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef Account_ID = new ColumnDef("Account_ID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef Account_OriginalID = new ColumnDef("Account_OriginalID", type: SqlDbType.NVarChar, size: 100, nullable: true);
				public static ColumnDef DownloadedDate = new ColumnDef("DownloadedDate", type: SqlDbType.DateTime, nullable: true, defaultValue: "GetDate()");				
				public static ColumnDef TargetPeriodStart = new ColumnDef("TargetPeriodStart", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef TargetPeriodEnd = new ColumnDef("TargetPeriodEnd", type: SqlDbType.DateTime, nullable: false);

			}
			public class MetricsDimensionSegment
			{
				public static ColumnDef MetricsUsid = new ColumnDef("MetricsUsid", size: 32, type: SqlDbType.Char, nullable: false);
				public static ColumnDef SegmentID = new ColumnDef("SegmentID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef TypeID = new ColumnDef("TypeID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 4000);
				public static ColumnDef Status = new ColumnDef("Status", type: SqlDbType.Int, nullable:true);
				public static ColumnDef Value = new ColumnDef("Value", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public class MetricsDimensionTarget
			{
				public static ColumnDef MetricsUsid = new ColumnDef("MetricsUsid", size: 32, type: SqlDbType.Char, nullable: false);
				public static ColumnDef TypeID = new ColumnDef("TypeID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef Status = new ColumnDef("Status", type: SqlDbType.Int);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}
		}
		/*=========================*/
		#endregion

		public GenericMetricsImportManager(long serviceInstanceID, ImportManagerOptions options = null)
			: base(serviceInstanceID, options)
		{
		}

		public override void ImportMetrics(GenericMetricsUnit metrics)
		{
			// Metrics
			var metricsRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Metrics.MetricsUsid, metrics.Usid.ToString("N")},
				{Tables.Metrics.TargetPeriodStart, metrics.PeriodStart},
				{Tables.Metrics.TargetPeriodEnd, metrics.PeriodEnd},
				{Tables.Metrics.Account_ID, metrics.Account.ID},
				{Tables.Metrics.Account_OriginalID, metrics.Account.OriginalID},
				{Tables.Metrics.Channel_ID, metrics.Channel.ID}
			};

			foreach (KeyValuePair<Measure, double> measure in metrics.MeasureValues)
			{
				// Use the Oltp name of the measure as the column name
				metricsRow[new ColumnDef(measure.Key.Name)] = measure.Value;
			}
			Bulk<Tables.Metrics>().SubmitRow(metricsRow);

			// MetricsDimensionSegment
			foreach (var segment in metrics.SegmentDimensions)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.MetricsDimensionSegment.MetricsUsid, metrics.Usid.ToString("N") },
					{ Tables.MetricsDimensionSegment.SegmentID, segment.Key.ID},
					{ Tables.MetricsDimensionSegment.TypeID, segment.Value.TypeID },
					{ Tables.MetricsDimensionSegment.OriginalID, segment.Value.OriginalID },
					{ Tables.MetricsDimensionSegment.Status, segment.Value.Status },
					{ Tables.MetricsDimensionSegment.Value, segment.Value.Value }
				};

				foreach (KeyValuePair<MappedObjectField, object> fixedField in segment.Value.GetFieldValues())
					row[new ColumnDef(Tables.MetricsDimensionTarget.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> customField in segment.Value.ExtraFields)
					row[new ColumnDef(Tables.MetricsDimensionTarget.ExtraFieldX, customField.Key.ColumnIndex)] = customField.Value;

				Bulk<Tables.MetricsDimensionSegment>().SubmitRow(row);
			}

			// MetricsDimensionTarget
			foreach (Target target in metrics.TargetDimensions)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.MetricsDimensionTarget.MetricsUsid, metrics.Usid.ToString("N") },
					{ Tables.MetricsDimensionTarget.TypeID, target.TypeID },
					{ Tables.MetricsDimensionTarget.OriginalID, target.OriginalID },
					{ Tables.MetricsDimensionTarget.Status, target.Status },
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
			get { return "GEN"; }
		}

		protected override Type MetricsTableDefinition
		{
			get { return typeof(Tables.Metrics); }
		}
	}
}
