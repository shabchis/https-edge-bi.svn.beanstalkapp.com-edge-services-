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
		public Dictionary<Segment, SegmentValue> Segments { get; private set; }

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
				public static ColumnDef DownloadedDate = new ColumnDef("DownloadedDate", type: SqlDbType.DateTime, nullable: true, defaultValue: "GetDate()");				
				public static ColumnDef TargetPeriodStart = new ColumnDef("TargetPeriodStart", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef TargetPeriodEnd = new ColumnDef("TargetPeriodEnd", type: SqlDbType.DateTime, nullable: false);

				//TO DO : Measures ???
			}
			public static class Segment
			{
				public static ColumnDef Usid = new ColumnDef("Usid", size: 100, nullable: false);
				public static ColumnDef AccountID = new ColumnDef("AccountID", type: SqlDbType.BigInt);				
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
					measuresValidationSQL.AppendFormat("{1}SUM([{0}]) as [{2}]", measure.Name, measuresValidationSQL.Length > 0 ? ", " : null,measure.SourceName);

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

		public void ImportMetrics(SegmentMetricsUnit metrics, string boUsid)
		{
			if (this.State != DeliveryImportManagerState.Importing)
				throw new InvalidOperationException("BeginImport must be called before anything can be imported.");

			// Metrics
			var metricsRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Metrics.Usid, boUsid},
				{Tables.Metrics.TargetPeriodStart, metrics.PeriodStart},
				{Tables.Metrics.TargetPeriodEnd, metrics.PeriodEnd}
				
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
					{Tables.Segment.AccountID, CurrentDelivery.Account.ID},					
					{ Tables.Segment.SegmentID, segment.Key.ID },
					{ Tables.Segment.Value, segment.Value.Value },
					{ Tables.Segment.ValueOriginalID, segment.Value.OriginalID }
				});
			}
		}
		protected override void OnEndImport()
		{
			_bulkMetrics.Flush();
			_bulkBoSegment.Flush();
		}
		/*=========================*/
		#endregion
		
		#region Prepare
		/*=========================*/
		SqlCommand _prepareCommand = null;
		SqlCommand _validateCommand = null;
		const int Prepare_PREPARE_PASS = 0;
		const int Prepare_VALIDATE_PASS = 1;
		const string ValidationTable = "Commit_FinalMetrics";

		protected override int PreparePassCount
		{
			get { return 2; }
		}

		protected override void OnBeginPrepare()
		{
			if (String.IsNullOrWhiteSpace(this.Options.SqlPrepareCommand))
				throw new ConfigurationException("Options.SqlPrepareCommand is empty.");

			_sqlConnection = NewDeliveryDbConnection();
			_sqlConnection.Open();
		}

		protected override void OnPrepare(int pass)
		{
			DeliveryHistoryEntry processedEntry = this.CurrentDelivery.History.Last(entry => entry.Operation == DeliveryOperation.Imported);
			if (processedEntry == null)
				throw new Exception("This delivery has not been imported yet (could not find an 'Imported' history entry).");

			// get this from last 'Processed' history entry
			string measuresFieldNamesSQL = processedEntry.Parameters[Consts.DeliveryHistoryParameters.MeasureOltpFieldsSql].ToString();
			string measuresNamesSQL = processedEntry.Parameters[Consts.DeliveryHistoryParameters.MeasureNamesSql].ToString();

			string tablePerfix = processedEntry.Parameters[Consts.DeliveryHistoryParameters.TablePerfix].ToString();
			string deliveryId = this.CurrentDelivery.DeliveryID.ToString("N");

			if (pass == Prepare_PREPARE_PASS)
			{
				// ...........................
				// PREPARE data

				_prepareCommand = _prepareCommand ?? DataManager.CreateCommand(this.Options.SqlPrepareCommand, CommandType.StoredProcedure);
				_prepareCommand.Connection = _sqlConnection;

				_prepareCommand.Parameters["@DeliveryID"].Size = 4000;
				_prepareCommand.Parameters["@DeliveryID"].Value = deliveryId;
				_prepareCommand.Parameters["@DeliveryTablePrefix"].Size = 4000;
				_prepareCommand.Parameters["@DeliveryTablePrefix"].Value = tablePerfix;
				_prepareCommand.Parameters["@MeasuresNamesSQL"].Size = 4000;
				_prepareCommand.Parameters["@MeasuresNamesSQL"].Value = measuresNamesSQL;
				_prepareCommand.Parameters["@MeasuresFieldNamesSQL"].Size = 4000;
				_prepareCommand.Parameters["@MeasuresFieldNamesSQL"].Value = measuresFieldNamesSQL;
				_prepareCommand.Parameters["@CommitTableName"].Size = 4000;
				_prepareCommand.Parameters["@CommitTableName"].Direction = ParameterDirection.Output;

				try { _prepareCommand.ExecuteNonQuery(); }
				catch (Exception ex)
				{
					throw new Exception(String.Format("Delivery {0} failed during Prepare.", deliveryId), ex);
				}

				this.HistoryEntryParameters[Consts.DeliveryHistoryParameters.CommitTableName] = _prepareCommand.Parameters["@CommitTableName"].Value;
			}
			else if (pass == Prepare_VALIDATE_PASS)
			{
				object totalso;

				if (processedEntry.Parameters.TryGetValue(Consts.DeliveryHistoryParameters.ChecksumTotals, out totalso))
				{
					var totals = (Dictionary<string, double>)totalso;

					object sql;
					if (processedEntry.Parameters.TryGetValue(Consts.DeliveryHistoryParameters.MeasureValidateSql, out sql))
					{

						string measuresValidateSQL = (string)sql;
						measuresValidateSQL = measuresValidateSQL.Insert(0, "SELECT ");
						measuresValidateSQL = measuresValidateSQL + string.Format("\nFROM {0}_{1} \nWHERE DeliveryID=@DeliveryID:Nvarchar", tablePerfix, ValidationTable);

						SqlCommand validateCommand = DataManager.CreateCommand(measuresValidateSQL);
						validateCommand.Connection = _sqlConnection;
						validateCommand.Parameters["@DeliveryID"].Value = this.CurrentDelivery.DeliveryID.ToString("N");
						using (SqlDataReader reader = validateCommand.ExecuteReader())
						{
							if (reader.Read())
							{
								var results = new StringBuilder();
								foreach (KeyValuePair<string, double> total in totals)
								{
									if (reader[total.Key] is DBNull)
									{

										if (total.Value == 0)
											Log.Write(string.Format("[zero totals] {0} has no data or total is 0 in table {1} for target period {2}", total.Key, ValidationTable, CurrentDelivery.TargetPeriod), LogMessageType.Information);
										else
											results.AppendFormat("{0} is null in table {1}\n but {2} in measure {3}", total.Key, ValidationTable, total.Key, total.Value);
									}
									else
									{
										double val = Convert.ToDouble(reader[total.Key]);
										double diff = Math.Abs((total.Value - val) / total.Value);
										if (diff > this.Options.CommitValidationThreshold)
											results.AppendFormat("{0}: processor totals = {1}, {2} table = {3}\n", total.Key, total.Value, ValidationTable, val);
										else if (val == 0 && total.Value == 0)
											Log.Write(string.Format("[zero totals] {0} has no data or total is 0 in table {1} for target period {2}", total.Key, ValidationTable, CurrentDelivery.TargetPeriod), LogMessageType.Information);


									}
								}
								if (results.Length > 0)
									throw new Exception("Commit validation (checksum) failed:\n" + results.ToString());
							}
							else
								throw new Exception(String.Format("Commit validation (checksum) did not find any data matching this delivery in {0}.", ValidationTable));
						}
					}
				}
			}
		}
		#endregion

		#region Commit
		/*=========================*/

		SqlTransaction _commitTransaction = null;
		SqlCommand _commitCommand = null;

		protected override int CommitPassCount
		{
			get { return 1; }
		}

		protected override void OnBeginCommit()
		{
			if (String.IsNullOrWhiteSpace(this.Options.SqlCommitCommand))
				throw new ConfigurationException("Options.SqlCommitCommand is empty.");

			_sqlConnection = NewDeliveryDbConnection();
			_sqlConnection.Open();

			_commitTransaction = _sqlConnection.BeginTransaction("Delivery Commit");
		}

		protected override void OnCommit(int pass)
		{
			DeliveryHistoryEntry processedEntry = this.CurrentDelivery.History.Last(entry => entry.Operation == DeliveryOperation.Imported);
			if (processedEntry == null)
				throw new Exception("This delivery has not been imported yet (could not find an 'Imported' history entry).");

			DeliveryHistoryEntry preparedEntry = this.CurrentDelivery.History.Last(entry => entry.Operation == DeliveryOperation.Prepared);
			if (preparedEntry == null)
				throw new Exception("This delivery has not been prepared yet (could not find an 'Prepared' history entry).");

			// get this from last 'Processed' history entry
			string measuresFieldNamesSQL = processedEntry.Parameters[Consts.DeliveryHistoryParameters.MeasureOltpFieldsSql].ToString();
			string measuresNamesSQL = processedEntry.Parameters[Consts.DeliveryHistoryParameters.MeasureNamesSql].ToString();

			string tablePerfix = processedEntry.Parameters[Consts.DeliveryHistoryParameters.TablePerfix].ToString();
			string deliveryId = this.CurrentDelivery.DeliveryID.ToString("N");


			// ...........................
			// COMMIT data to OLTP

			_commitCommand = _commitCommand ?? DataManager.CreateCommand(this.Options.SqlCommitCommand, CommandType.StoredProcedure);
			_commitCommand.Connection = _sqlConnection;
			_commitCommand.Transaction = _commitTransaction;

			_commitCommand.Parameters["@DeliveryFileName"].Size = 4000;
			_commitCommand.Parameters["@DeliveryFileName"].Value = tablePerfix;
			_commitCommand.Parameters["@CommitTableName"].Size = 4000;
			_commitCommand.Parameters["@CommitTableName"].Value = preparedEntry.Parameters["CommitTableName"];
			_commitCommand.Parameters["@MeasuresNamesSQL"].Size = 4000;
			_commitCommand.Parameters["@MeasuresNamesSQL"].Value = measuresNamesSQL;
			_commitCommand.Parameters["@MeasuresFieldNamesSQL"].Size = 4000;
			_commitCommand.Parameters["@MeasuresFieldNamesSQL"].Value = measuresFieldNamesSQL;
			_commitCommand.Parameters["@Signature"].Size = 4000;
			_commitCommand.Parameters["@Signature"].Value = this.CurrentDelivery.Signature; ;
			_commitCommand.Parameters["@DeliveryIDsPerSignature"].Size = 4000;
			_commitCommand.Parameters["@DeliveryIDsPerSignature"].Direction = ParameterDirection.Output;
			_commitCommand.Parameters["@DeliveryID"].Size = 4000;
			_commitCommand.Parameters["@DeliveryID"].Value = deliveryId;



			try
			{
				_commitCommand.ExecuteNonQuery();
				//	_commitTransaction.Commit();

				string deliveryIDsPerSignature = _commitCommand.Parameters["@DeliveryIDsPerSignature"].Value.ToString();

				string[] existDeliveries;
				if ((!string.IsNullOrEmpty(deliveryIDsPerSignature) && deliveryIDsPerSignature != "0"))
				{
					_commitTransaction.Rollback();
					existDeliveries = deliveryIDsPerSignature.Split(',');
					List<Delivery> deliveries = new List<Delivery>();
					foreach (string existDelivery in existDeliveries)
					{
						deliveries.Add(Delivery.Get(Guid.Parse(existDelivery)));
					}
					throw new DeliveryConflictException(string.Format("deliveries with the same signature already comitted in database\n deliveries:\n {0}:", deliveryIDsPerSignature)) { ConflictingDeliveries = deliveries.ToArray() };





				}
				else
					//already updated by sp, this is so we don't override it
					this.CurrentDelivery.IsCommited = true;
			}
			finally
			{
				this.State = DeliveryImportManagerState.Idle;
			}
		}

		protected override void OnEndCommit(Exception ex)
		{
			if (_commitTransaction != null)
			{
				if (ex == null)
					_commitTransaction.Commit();
				else
					_commitTransaction.Rollback();
			}
			this.State = DeliveryImportManagerState.Idle;
		}

		protected override void OnDisposeCommit()
		{
			if (_commitTransaction != null)
				_commitTransaction.Dispose();
		}

		/*=========================*/
		#endregion

		#region Rollback
		/*=========================*/

		SqlCommand _rollbackCommand = null;
		SqlTransaction _rollbackTransaction = null;
		protected override void OnBeginRollback()
		{
			if (String.IsNullOrWhiteSpace(this.Options.SqlRollbackCommand))
				throw new ConfigurationException("Options.SqlRollbackCommand is empty.");

			_sqlConnection = NewDeliveryDbConnection();
			_sqlConnection.Open();
			_rollbackTransaction = _sqlConnection.BeginTransaction("Delivery Rollback");
		}

		protected override void OnRollback(int pass)
		{
			DeliveryHistoryEntry prepareEntry = null;
			string guid = this.CurrentDelivery.DeliveryID.ToString("N");
			IEnumerable<DeliveryHistoryEntry> prepareEntries = this.CurrentDelivery.History.Where(entry => entry.Operation == DeliveryOperation.Prepared);
			if (prepareEntries != null && prepareEntries.Count() > 0)
				prepareEntry = (DeliveryHistoryEntry)prepareEntries.Last();
			if (prepareEntry == null)
				throw new Exception(String.Format("The delivery '{0}' has never been comitted so it cannot be rolled back.", guid));

			_rollbackCommand = _rollbackCommand ?? DataManager.CreateCommand(this.Options.SqlRollbackCommand, CommandType.StoredProcedure);
			_rollbackCommand.Connection = _sqlConnection;
			_rollbackCommand.Transaction = _rollbackTransaction;

			_rollbackCommand.Parameters["@DeliveryID"].Value = guid;
			_rollbackCommand.Parameters["@TableName"].Value = prepareEntry.Parameters[Consts.DeliveryHistoryParameters.CommitTableName];

			_rollbackCommand.ExecuteNonQuery();
			this.CurrentDelivery.IsCommited = false;
		}

		protected override void OnEndRollback(Exception ex)
		{
			if (ex == null)
				_rollbackTransaction.Commit();
			else
				_rollbackTransaction.Rollback();
		}

		protected override void OnDisposeRollback()
		{
			if (_rollbackTransaction != null)
				_rollbackTransaction.Dispose();
		}
		#endregion

		#region Misc
		SqlConnection NewDeliveryDbConnection()
		{
			return new SqlConnection(AppSettings.GetConnectionString(typeof(Delivery), Delivery.Consts.ConnectionStrings.SqlStagingDatabase));
		}
		protected override void OnDispose()
		{
			if (_sqlConnection != null)
				_sqlConnection.Dispose();
		}


		#endregion
	}
}
