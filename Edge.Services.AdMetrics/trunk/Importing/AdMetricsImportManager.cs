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
	public class AdMetricsImportManager : DeliveryImportManager, IDisposable
	{
		#region Table structure
		/*=========================*/

		private static class Tables
		{
			public static class Ad
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef Name = new ColumnDef("Name", size: 100);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef Campaign_Account_ID = new ColumnDef("Campaign_Account_ID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef Campaign_Account_OriginalID = new ColumnDef("Campaign_Account_OriginalID", type: SqlDbType.NVarChar, size: 100, nullable: false);
				public static ColumnDef Campaign_Channel = new ColumnDef("Campaign_Channel", type: SqlDbType.Int, nullable: false);
				public static ColumnDef Campaign_Name = new ColumnDef("Campaign_Name", size: 100, nullable: false);
				public static ColumnDef Campaign_OriginalID = new ColumnDef("Campaign_OriginalID", size: 100, nullable: false);
				public static ColumnDef Campaign_Status = new ColumnDef("Campaign_Status", type: SqlDbType.Int);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			public static class AdCreative
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef Name = new ColumnDef("Name", size: 100);
				public static ColumnDef CreativeType = new ColumnDef("CreativeType", type: SqlDbType.Int);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);

				/// <summary>
				/// Reserved for post-processing
				/// </summary>
				public static ColumnDef CreativeGK = new ColumnDef("CreativeGK", size: 50, nullable: true);

				/// <summary>
				/// Reserved for post-processing
				/// </summary>
				public static ColumnDef PpcCreativeGK = new ColumnDef("PpcCreativeGK", size: 50, nullable: true);
			}

			public static class AdTarget
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef TargetType = new ColumnDef("TargetType", type: SqlDbType.Int);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);
			}

			// TODO: flatten
			public static class AdSegment
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef SegmentID = new ColumnDef("SegmentID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef ValueOriginalID = new ColumnDef("ValueOriginalID", size: 4000);
				public static ColumnDef Value = new ColumnDef("Value", size: 4000);
			}

			public static class Metrics
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef MetricsUnitGuid = new ColumnDef("MetricsUnitGuid", size: 300, nullable: false);
				public static ColumnDef DownloadedDate = new ColumnDef("DownloadedDate", type: SqlDbType.DateTime, nullable: true, defaultValue: "GetDate()");
				public static ColumnDef TargetPeriodStart = new ColumnDef("TargetPeriodStart", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef TargetPeriodEnd = new ColumnDef("TargetPeriodEnd", type: SqlDbType.DateTime, nullable: false);
				public static ColumnDef Currency = new ColumnDef("Currency", size: 10);
			}

			public static class MetricsTargetMatch
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef MetricsUnitGuid = new ColumnDef("MetricsUnitGuid", size: 300, nullable: false);
				public static ColumnDef OriginalID = new ColumnDef("OriginalID", size: 100);
				public static ColumnDef TargetType = new ColumnDef("TargetType", type: SqlDbType.Int);
				public static ColumnDef DestinationUrl = new ColumnDef("DestinationUrl", size: 4000);
				public static ColumnDef FieldX = new ColumnDef("Field{0}", type: SqlDbType.NVarChar, size: 4000, copies: 4);
				public static ColumnDef ExtraFieldX = new ColumnDef("ExtraField{0}", type: SqlDbType.NVarChar, copies: 6, size: 4000);

				/// <summary>
				/// Reserved for post-processing
				/// </summary>
				public static ColumnDef KeywordGK = new ColumnDef("KeywordGK", type: SqlDbType.NChar, size: 50, nullable: true);

				/// <summary>
				/// Reserved for post-processing
				/// </summary>
				public static ColumnDef PpcKeywordGK = new ColumnDef("PpcKeywordGK", type: SqlDbType.NChar, size: 50, nullable: true);
			}

			public static class MetricsTargetMatchSegment
			{
				public static ColumnDef AdUsid = new ColumnDef("AdUsid", size: 100, nullable: false);
				public static ColumnDef MetricsUnitGuid = new ColumnDef("MetricsUnitGuid", size: 300, nullable: false);
				public static ColumnDef SegmentID = new ColumnDef("SegmentID", type: SqlDbType.Int, nullable: false);
				public static ColumnDef ValueOriginalID = new ColumnDef("ValueOriginalID", size: 4000);
				public static ColumnDef Value = new ColumnDef("Value", size: 4000);
			}

			
		}

		/*=========================*/
		#endregion

		#region Fields
		/*=========================*/

		private SqlConnection _sqlConnection;
		public Dictionary<string, Measure> Measures { get; private set; }
		public ImportManagerOptions Options { get; private set; }

		/*=========================*/
		#endregion

		#region Constructors
		/*=========================*/

		public AdMetricsImportManager(long serviceInstanceID, ImportManagerOptions options = null)
			: base(serviceInstanceID)
		{
			options = options ?? new ImportManagerOptions();
			options.SqlOltpConnectionString = options.SqlOltpConnectionString ?? AppSettings.GetConnectionString(this, Consts.ConnectionStrings.Oltp);
			options.SqlPrepareCommand = options.SqlPrepareCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlPrepareCommand, throwException: false);
			options.SqlCommitCommand = options.SqlCommitCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlCommitCommand, throwException: false);
			options.SqlRollbackCommand = options.SqlRollbackCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlRollbackCommand, throwException: false);
			this.Options = options;
		}

		/*=========================*/
		#endregion

		#region Import
		/*=========================*/

		private BulkObjects _bulkAd;
		private BulkObjects _bulkAdSegment;
		private BulkObjects _bulkAdTarget;
		private BulkObjects _bulkAdCreative;
		private BulkObjects _bulkMetrics;
		private BulkObjects _bulkMetricsTargetMatch;
		private BulkObjects _bulkMetricsTargetMatchSegment;
		private string _tablePrefix;

		public Func<Ad, long> OnAdUsidRequired = null;

		protected override void OnBeginImport()
		{
			this._tablePrefix = string.Format("AD_{0}_{1}_{2}", this.CurrentDelivery.Account.ID, DateTime.Now.ToString("yyyMMdd_HHmmss"), this.CurrentDelivery.DeliveryID.ToString("N").ToLower());
			this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.TablePerfix, this._tablePrefix);

			int bufferSize = int.Parse(AppSettings.Get(this, Consts.AppSettings.BufferSize));

			// Connect to database
			_sqlConnection = NewDeliveryDbConnection();
			_sqlConnection.Open();
			
			_bulkAd = new BulkObjects(this._tablePrefix, typeof(Tables.Ad), _sqlConnection, bufferSize);
			_bulkAdSegment = new BulkObjects(this._tablePrefix, typeof(Tables.AdSegment), _sqlConnection, bufferSize);
			_bulkAdTarget = new BulkObjects(this._tablePrefix, typeof(Tables.AdTarget), _sqlConnection, bufferSize);
			_bulkAdCreative = new BulkObjects(this._tablePrefix, typeof(Tables.AdCreative), _sqlConnection, bufferSize);
			_bulkMetrics = new BulkObjects(this._tablePrefix, typeof(Tables.Metrics), _sqlConnection, bufferSize);
			_bulkMetricsTargetMatch = new BulkObjects(this._tablePrefix, typeof(Tables.MetricsTargetMatch), _sqlConnection, bufferSize);
			_bulkMetricsTargetMatchSegment = new BulkObjects(this._tablePrefix, typeof(Tables.MetricsTargetMatchSegment), _sqlConnection, bufferSize);

			// Get measures
			using (SqlConnection oltpConnection = new SqlConnection(this.Options.SqlOltpConnectionString))
			{
				oltpConnection.Open();

				this.Measures = Measure.GetMeasures(
					this.CurrentDelivery.Account,
					this.CurrentDelivery.Channel,
					oltpConnection,
					// NOT IsTarget and NOT IsCalculated and NOT IsBO
						MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
						MeasureOptionsOperator.Not
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

				if (measure.Options.HasFlag(MeasureOptions.ValidationRequired))
					measuresValidationSQL.AppendFormat("{1}SUM([{0}]) as [{0}]", measure.Name, measuresValidationSQL.Length > 0 ? ", " : null);

				count++;
			}


			this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.MeasureOltpFieldsSql, measuresFieldNamesSQL.ToString());
			this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.MeasureNamesSql, measuresNamesSQL.ToString());
			if (string.IsNullOrEmpty(measuresValidationSQL.ToString()))
				Log.Write("No measures marked for checksum validation; there will be no validation before the final commit.", LogMessageType.Warning);
			else
				this.HistoryEntryParameters.Add(Consts.DeliveryHistoryParameters.MeasureValidateSql, measuresValidationSQL.ToString());

			// Create the tables
			StringBuilder createTableCmdText = new StringBuilder();
			createTableCmdText.Append(_bulkAd.GetCreateTableSql());
			createTableCmdText.Append(_bulkAdSegment.GetCreateTableSql());
			createTableCmdText.Append(_bulkAdTarget.GetCreateTableSql());
			createTableCmdText.Append(_bulkAdCreative.GetCreateTableSql());
			createTableCmdText.Append(_bulkMetrics.GetCreateTableSql());
			createTableCmdText.Append(_bulkMetricsTargetMatch.GetCreateTableSql());
			createTableCmdText.Append(_bulkMetricsTargetMatchSegment.GetCreateTableSql());
			SqlCommand cmd = new SqlCommand(createTableCmdText.ToString(), _sqlConnection);
			cmd.CommandTimeout = 80; //DEFAULT IS 30 AND SOMTIME NOT ENOUGH WHEN RUNING CUBE
			cmd.ExecuteNonQuery();

		}

		public void ImportAd(Ad ad)
		{
			if (this.State != DeliveryImportManagerState.Importing)
				throw new InvalidOperationException("BeginImport must be called before anything can be imported.");

			string adUsid = GetAdIdentity(ad);

			// Ad
			var adRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Ad.AdUsid, adUsid},
				{Tables.Ad.Name, ad.Name},
				{Tables.Ad.OriginalID, ad.OriginalID},
				{Tables.Ad.DestinationUrl, ad.DestinationUrl},
				{Tables.Ad.Campaign_Account_ID, ad.Campaign.Account.ID},
				{Tables.Ad.Campaign_Account_OriginalID, ad.Campaign.Account.OriginalID},
				{Tables.Ad.Campaign_Channel, ad.Campaign.Channel.ID},
				{Tables.Ad.Campaign_Name, ad.Campaign.Name},
				{Tables.Ad.Campaign_OriginalID, ad.Campaign.OriginalID},
				{Tables.Ad.Campaign_Status, ad.Campaign.Status == ObjectStatus.Unknown ? (object) DBNull.Value : (object) ad.Campaign.Status},
			};
			foreach (KeyValuePair<ExtraField, object> extraField in ad.ExtraFields)
				adRow[new ColumnDef(Tables.AdTarget.ExtraFieldX, extraField.Key.ColumnIndex)] = extraField.Value;

			_bulkAd.SubmitRow(adRow);

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

				foreach (KeyValuePair<MappedFieldMetadata, object> fixedField in target.GetFieldValues())
					row[new ColumnDef(Tables.AdTarget.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> extraField in target.ExtraFields)
					row[new ColumnDef(Tables.AdTarget.ExtraFieldX, extraField.Key.ColumnIndex)] = extraField.Value;

				_bulkAdTarget.SubmitRow(row);
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

				foreach (KeyValuePair<MappedFieldMetadata, object> fixedField in creative.GetFieldValues())
					row[new ColumnDef(Tables.AdTarget.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				_bulkAdCreative.SubmitRow(row);
			}

			// AdSegment
			foreach (KeyValuePair<Segment, SegmentValue> segment in ad.Segments)
			{
				_bulkAdSegment.SubmitRow(new Dictionary<ColumnDef, object>()
				{
					{ Tables.AdSegment.AdUsid, adUsid },
					{ Tables.AdSegment.SegmentID, segment.Key.ID },
					{ Tables.AdSegment.Value, segment.Value.Value },
					{ Tables.AdSegment.ValueOriginalID, segment.Value.OriginalID }
				});
			}
		}


		public void ImportMetrics(AdMetricsUnit metrics)
		{
			if (this.State != DeliveryImportManagerState.Importing)
				throw new InvalidOperationException("BeginImport must be called before anything can be imported.");

			if (metrics.Ad == null)
				throw new InvalidOperationException("Cannot import a metrics unit that is not associated with an ad.");

			string adUsid = GetAdIdentity(metrics.Ad);
			string metricsGuid = metrics.Guid.ToString("N");

			// Metrics
			var metricsRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Metrics.MetricsUnitGuid, metricsGuid},
				{Tables.Metrics.AdUsid, adUsid},
				{Tables.Metrics.TargetPeriodStart, metrics.PeriodStart},
				{Tables.Metrics.TargetPeriodEnd, metrics.PeriodEnd},
				{Tables.Metrics.Currency, metrics.Currency == null ? null : metrics.Currency.Code}
			};

			foreach (KeyValuePair<Measure, double> measure in metrics.MeasureValues)
			{
				// Use the Oltp name of the measure as the column name
				metricsRow[new ColumnDef(measure.Key.Name)] = measure.Value;
			}

			_bulkMetrics.SubmitRow(metricsRow);

			// MetricsTargetMatch
			// TODO: this shouldn't just duplicate ad targets - find a different solution
			foreach (Target target in metrics.TargetMatches)
			{
				var row = new Dictionary<ColumnDef, object>()
				{
					{ Tables.MetricsTargetMatch.MetricsUnitGuid, metricsGuid },
					{ Tables.MetricsTargetMatch.AdUsid, adUsid },
					{ Tables.MetricsTargetMatch.OriginalID, target.OriginalID },
					{ Tables.MetricsTargetMatch.DestinationUrl, target.DestinationUrl },
					{ Tables.MetricsTargetMatch.TargetType, target.TypeID }
				};

				foreach (KeyValuePair<MappedFieldMetadata, object> fixedField in target.GetFieldValues())
					row[new ColumnDef(Tables.AdTarget.FieldX, fixedField.Key.ColumnIndex)] = fixedField.Value;

				foreach (KeyValuePair<ExtraField, object> customField in target.ExtraFields)
					row[new ColumnDef(Tables.AdTarget.ExtraFieldX, customField.Key.ColumnIndex)] = customField.Value;

				// AdSegment
				if (target.Segments != null)
				{
					foreach (KeyValuePair<Segment, SegmentValue> segment in target.Segments)
					{
						_bulkMetricsTargetMatchSegment.SubmitRow(new Dictionary<ColumnDef, object>()
					{
						{ Tables.MetricsTargetMatchSegment.MetricsUnitGuid, metricsGuid },
						{ Tables.MetricsTargetMatchSegment.AdUsid, adUsid },
						{ Tables.MetricsTargetMatchSegment.SegmentID, segment.Key.ID },
						{ Tables.MetricsTargetMatchSegment.Value, segment.Value.Value },
						{ Tables.MetricsTargetMatchSegment.ValueOriginalID, segment.Value.OriginalID }
					});
					}
				}

				_bulkMetricsTargetMatch.SubmitRow(row);
			}

		}



		private string GetAdIdentity(Ad ad)
		{
			string val;
			if (this.OnAdUsidRequired != null)
				val = this.OnAdUsidRequired(ad).ToString();
			else if (String.IsNullOrEmpty(ad.OriginalID))
				throw new Exception("Ad.OriginalID is required. If it is not available, provide a function for AdDataImportSession.OnAdIdentityRequired that returns a unique value for this ad.");
			else
				val = ad.OriginalID.ToString();

			return val;
		}

		protected override void OnEndImport()
		{
			_bulkAd.Flush();
			_bulkAdCreative.Flush();
			_bulkAdTarget.Flush();
			_bulkAdSegment.Flush();
			_bulkMetrics.Flush();
			_bulkMetricsTargetMatch.Flush();
			_bulkMetricsTargetMatchSegment.Flush();
		}

		protected override void OnDisposeImport()
		{
			if (_bulkAd != null)
			{
				_bulkAd.Dispose();
				_bulkAdCreative.Dispose();
				_bulkAdTarget.Dispose();
				_bulkAdSegment.Dispose();
				_bulkMetrics.Dispose();
				_bulkMetricsTargetMatch.Dispose();
				_bulkMetricsTargetMatchSegment.Dispose();
			}
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

		/*=========================*/
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

		/*=========================*/
		#endregion

		#region Misc
		/*=========================*/

		SqlConnection NewDeliveryDbConnection()
		{
			return new SqlConnection(AppSettings.GetConnectionString(typeof(Delivery), Delivery.Consts.ConnectionStrings.SqlStagingDatabase));
		}

		protected override void OnDispose()
		{
			if (_sqlConnection != null)
				_sqlConnection.Dispose();
		}

		/*=========================*/
		#endregion
	}
}
