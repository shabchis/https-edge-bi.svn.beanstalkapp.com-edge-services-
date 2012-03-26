﻿using System;
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

namespace Edge.Services.SegmentMetrics
{
	public class GenericMetricsImportManager : MetricsImportManager<GenericMetricsUnit>
	{
		#region Table structure
		/*=========================*/
		private static class Tables
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
				public static ColumnDef SegmentType = new ColumnDef("SegmentType", type: SqlDbType.Int, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 4000);
				public static ColumnDef Value = new ColumnDef("Value", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public class MetricsDimensionTarget
			{
				public static ColumnDef MetricsUsid = new ColumnDef("MetricsUsid", size: 32, type: SqlDbType.Char, nullable: false);
				public static ColumnDef TargetType = new ColumnDef("TargetType", type: SqlDbType.Int);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
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
				{Tables.Metrics.MetricsUsid, metrics.Usid},
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

			foreach (Segment segment in metrics.SegmentDimensions)
			{
			}
		}

		protected override string TablePrefixType
		{
			get { return "GEN"; }
		}

		protected override MeasureOptions MeasureOptions
		{
			get { throw new NotImplementedException(); }
		}

		protected override OptionsOperator MeasureOptionsOperator
		{
			get { throw new NotImplementedException(); }
		}

		protected override SegmentOptions SegmentOptions
		{
			get { throw new NotImplementedException(); }
		}

		protected override OptionsOperator SegmentOptionsOperator
		{
			get { throw new NotImplementedException(); }
		}

		protected override Type MetricsTableDefinition
		{
			get { return typeof(Tables.Metrics); }
		}
	}
}
