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

namespace Edge.Services.SegmentMetrics
{
	public class SegmentMetricsImportManager : DeliveryImportManager
	{

		private SqlConnection _sqlConnection;
		public ImportManagerOptions Options { get; private set; }
		public Dictionary<string, Measure> Measures { get; private set; }

		public SegmentMetricsImportManager(long serviceInstanceID, ImportManagerOptions options = null)
			: base(serviceInstanceID)
		{
			options = options ?? new ImportManagerOptions();
			options.SqlOltpConnectionString = options.SqlOltpConnectionString ?? AppSettings.GetConnectionString(this, Consts.ConnectionStrings.Oltp);
			options.SqlPrepareCommand = options.SqlPrepareCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlPrepareCommand, throwException: false);
			options.SqlCommitCommand = options.SqlCommitCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlCommitCommand, throwException: false);
			options.SqlRollbackCommand = options.SqlRollbackCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlRollbackCommand, throwException: false);
			this.Options = options;
		}

		#region Table structure
		/*=========================*/
		private static class Tables
		{

			public static class Metrics
			{
		
				public static ColumnDef Usid = new ColumnDef("Usid", size: 100, nullable: false);
				public static ColumnDef DownloadedDate = new ColumnDef("DownloadedDate", type: SqlDbType.DateTime, nullable: false, defaultValue: "GetDate()");
				public static ColumnDef AccountID = new ColumnDef("Account_ID", type: SqlDbType.BigInt);
				public static ColumnDef TargetPeriodStart = new ColumnDef("TargetPeriodStart", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef TargetPeriodEnd = new ColumnDef("TargetPeriodEnd", type: SqlDbType.DateTime, nullable: false);

				//TO DO : Measures ???
			}
			public static class Segment
			{
				public static ColumnDef Usid = new ColumnDef("Usid", size: 100, nullable: false);
				public static ColumnDef SegmentID = new ColumnDef("SegmentID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef ValueOriginalID = new ColumnDef("ValueOriginalID", size: 4000);
				public static ColumnDef Value = new ColumnDef("Value", size: 4000);
			}
		}
		/*=========================*/
		#endregion

		#region Import
		/*=========================*/
		private string _tablePrefix;
		private BulkObjects _bulkMetrics;
		private BulkObjects _bulkBoSegment;

		protected override void OnBeginImport()
		{
			this._tablePrefix = string.Format("SEG_{0}_{1}_{2}", this.CurrentDelivery.Account.ID, DateTime.Now.ToString("yyyMMdd_HHmmss"), this.CurrentDelivery.DeliveryID.ToString("N").ToLower());
			this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.TablePerfix, this._tablePrefix);

			int bufferSize = int.Parse(AppSettings.Get(this, Consts.AppSettings.BufferSize));

			// Connect to database
			_sqlConnection = NewDeliveryDbConnection();
			_sqlConnection.Open();

			_bulkMetrics = new BulkObjects(this._tablePrefix, typeof(Tables.Metrics), _sqlConnection, bufferSize);
			_bulkBoSegment = new BulkObjects(this._tablePrefix, typeof(Tables.Segment), _sqlConnection, bufferSize);

			// Get measures
			using (SqlConnection oltpConnection = new SqlConnection(this.Options.SqlOltpConnectionString))
			{
				oltpConnection.Open();

				this.Measures = Measure.GetMeasures(
					this.CurrentDelivery.Account,
					this.CurrentDelivery.Channel,
					oltpConnection,
					MeasureOptions.IsBackOffice, MeasureOptionsOperator.And // TODO: IsBackOffice needs to become something more generic
					);
			}

			// Add measure columns to metrics,create measuresFieldNamesSQL,measuresNamesSQL
			StringBuilder measuresFieldNamesSQL = new StringBuilder(",");
			StringBuilder measuresNamesSQL = new StringBuilder(",");
			StringBuilder measuresValidationSQL = new StringBuilder();
			int count = 0;
			foreach (Measure measure in this.Measures.Values)
			{
				_bulkMetrics.AddColumn(new ColumnDef(
					name: measure.Name,
					type: SqlDbType.Float,
					nullable: true
					));

				measuresFieldNamesSQL.AppendFormat("[{0}]{1}", measure.OltpName, count < this.Measures.Values.Count - 1 ? "," : null);
				measuresNamesSQL.AppendFormat("[{0}]{1}", measure.Name, count < this.Measures.Values.Count - 1 ? "," : null);

				//TO ASK : DO WE NEED VALIDATION ? 
				if (measure.Options.HasFlag(MeasureOptions.ValidationRequired))
					measuresValidationSQL.AppendFormat("{1}SUM([{0}]) as [{0}]", measure.Name, measuresValidationSQL.Length > 0 ? ", " : null);

				count++;
			}

			this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.MeasureOltpFieldsSql, measuresFieldNamesSQL.ToString());
			this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.MeasureNamesSql, measuresNamesSQL.ToString());

			//Check sum validation fields
			if (string.IsNullOrEmpty(measuresValidationSQL.ToString()))
				Log.Write("No measures marked for checksum validation; there will be no validation before the final commit.", LogMessageType.Warning);
			else
				this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.MeasureValidateSql, measuresValidationSQL.ToString());

			// Create the tables
			StringBuilder createTableCmdText = new StringBuilder();
			createTableCmdText.Append(_bulkBoSegment.GetCreateTableSql());
			createTableCmdText.Append(_bulkMetrics.GetCreateTableSql());
			SqlCommand cmd = new SqlCommand(createTableCmdText.ToString(), _sqlConnection);
			cmd.ExecuteNonQuery();
		}
		/*=========================*/
		#endregion

		public void ImportMetrics(SegmentMetricsUnit metrics, string boUsid)
		{
			if (this.State != DeliveryImportManagerState.Importing)
				throw new InvalidOperationException("BeginImport must be called before anything can be imported.");

			// Metrics
			var metricsRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Metrics.Usid, boUsid},
				{Tables.Metrics.TargetPeriodStart, metrics.PeriodStart},
				{Tables.Metrics.TargetPeriodEnd, metrics.PeriodEnd},
			};
			
			foreach (KeyValuePair<Measure, double> measure in metrics.MeasureValues)
			{
				// Use the Oltp name of the measure as the column name
				metricsRow[new ColumnDef(measure.Key.Name)] = measure.Value;
			}
			_bulkMetrics.SubmitRow(metricsRow);

			foreach (KeyValuePair<Segment, SegmentValue> segment in metrics.Segments)
			{
				_bulkBoSegment.SubmitRow(new Dictionary<ColumnDef, object>()
				{
					{ Tables.Segment.Usid, boUsid },
					{ Tables.Segment.SegmentID, segment.Key.ID },
					{ Tables.Segment.Value, segment.Value.Value },
					{ Tables.Segment.ValueOriginalID, segment.Value.OriginalID }
				});
			}
		}

		SqlConnection NewDeliveryDbConnection()
		{
			return new SqlConnection(AppSettings.GetConnectionString(typeof(Delivery), Delivery.Consts.ConnectionStrings.SqlStagingDatabase));
		}

		protected override void OnPrepare(int pass)
		{
			throw new NotImplementedException();
		}

		protected override void OnCommit(int pass)
		{
			throw new NotImplementedException();
		}

		protected override void OnRollback(int pass)
		{
			throw new NotImplementedException();
		}
	}
}
