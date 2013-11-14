//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using Edge.Data.Pipeline;
//using Edge.Data.Pipeline.Common.Importing;
//using System.Data;
//using System.Data.SqlClient;
//using Edge.Data.Pipeline.Metrics;
//using Edge.Core.Configuration;
//using Edge.Data.Objects;
//using Edge.Core.Data;

//namespace Edge.Services.Currencies
//{
//    public class CurrencyImportManager: DeliveryImportManager
//    {



//        #region Fields
//        /*=========================*/

//        private SqlConnection _sqlConnection;
//        public MetricsImportManagerOptions Options { get; private set; }

//        /*=========================*/
//        #endregion

//        #region Table
//        /*=========================*/
//        private string _tablePrefix;

//        public static class Tables
//        {
//            public class Currency
//            {
//                public static ColumnDef RateSymbol = new ColumnDef("RateSymbol", type: SqlDbType.NVarChar, size: 100, nullable: false);
//                public static ColumnDef RateDate = new ColumnDef("RateDate", type: SqlDbType.DateTime, nullable: false);
//                public static ColumnDef RateValue = new ColumnDef("RateValue", type: SqlDbType.Float, nullable: false);
//                public static ColumnDef OutputID = new ColumnDef("OutputID", type: SqlDbType.Char,size: 32, nullable: false);
//            }
//        }
//        protected string TablePrefixType
//        {
//            get { return "Currency"; }
//        }
//        /*=========================*/
//        #endregion

//        #region Misc
//        /*=========================*/

//        SqlConnection NewDeliveryDbConnection()
//        {
//            return new SqlConnection(AppSettings.GetConnectionString(typeof(Delivery), Delivery.Consts.ConnectionStrings.SqlStagingDatabase));
//        }

//        protected override void OnDispose()
//        {
//            if (_sqlConnection != null)
//                _sqlConnection.Dispose();
//        }

//        /*=========================*/
//        #endregion

//        #region Import
//        /*=========================*/
//        private Dictionary<Type, BulkObjects> _bulks;
//        protected BulkObjects Bulk<TableDef>()
//        {
//            return _bulks[typeof(TableDef)];
//        }

//        protected override void OnBeginImport()
//        {
//            this._tablePrefix = string.Format("{0}_{1}_{2}", this.TablePrefixType, DateTime.Now.ToString("yyyMMdd_HHmmss"), this.CurrentDelivery.DeliveryID.ToString("N").ToLower());
//            this.CurrentDelivery.Parameters.Add(Consts.DeliveryHistoryParameters.TablePerfix, this._tablePrefix);

//            int bufferSize = int.Parse(AppSettings.Get(this, Consts.AppSettings.BufferSize));

//            // Connect to database
//            _sqlConnection = NewDeliveryDbConnection();
//            _sqlConnection.Open();

//            // Create bulk objects
//            _bulks = new Dictionary<Type, BulkObjects>();
//            Type tableList = this.GetType().GetNestedType("Tables", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
//            foreach (Type table in tableList.GetNestedTypes())
//            {
//                _bulks[table] = new BulkObjects(this._tablePrefix, table, _sqlConnection, bufferSize);
//            }

//            // Create the tables
//            StringBuilder createTableCmdText = new StringBuilder();
//            foreach (BulkObjects bulk in _bulks.Values)
//                createTableCmdText.Append(bulk.GetCreateTableSql());
//            SqlCommand cmd = new SqlCommand(createTableCmdText.ToString(), _sqlConnection);
//            cmd.CommandTimeout = 80; //DEFAULT IS 30 AND SOMTIME NOT ENOUGH WHEN RUNING CUBE
//            cmd.ExecuteNonQuery();
//        }
//        protected override void OnEndImport()
//        {
//            foreach (BulkObjects bulk in _bulks.Values)
//            {
//                bulk.Flush();
//                bulk.Dispose();
//            }
//        }
//        public void ImportCurrency(CurrencyRate currencyRate)
//        {
//            EnsureBeginImport();

//            var currencyRow = new Dictionary<ColumnDef, object>()
//            {
//                {Tables.Currency.RateSymbol,currencyRate.Currency.Code},
//                {Tables.Currency.RateValue,currencyRate.RateValue},
//                {Tables.Currency.RateDate,currencyRate.RateDate},
//                {Tables.Currency.OutputID,currencyRate.Output.OutputID.ToString("N")}
//            };


//            Bulk<Tables.Currency>().SubmitRow(currencyRow);

//        }
//        protected void EnsureBeginImport()
//        {
//            if (this.State != DeliveryImportManagerState.Importing)
//                throw new InvalidOperationException("BeginImport must be called before anything can be imported.");
//        }

//        /*=========================*/
//        #endregion

//        public CurrencyImportManager(long serviceInstanceID, MetricsImportManagerOptions options)
//            : base(serviceInstanceID)
//        {
//            options = options ?? new MetricsImportManagerOptions();
//            options.StagingConnectionString = options.StagingConnectionString ?? AppSettings.GetConnectionString(this, Consts.ConnectionStrings.StagingDatabase);
//            options.SqlTransformCommand = options.SqlTransformCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlTransformCommand, throwException: false);
//            options.SqlStageCommand = options.SqlStageCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlStageCommand, throwException: false);
//            options.SqlRollbackCommand = options.SqlRollbackCommand ?? AppSettings.Get(this, Consts.AppSettings.SqlRollbackCommand, throwException: false);

//            this.Options = options;
//        }

//        protected override void OnTransform(Delivery delivery, int pass)
//        {
//            throw new NotImplementedException();
//        }
//        protected override void OnStage(Delivery delivery, int pass)
//        {
//            throw new NotImplementedException();
//        }

//        SqlCommand _stageCommand = null;
//        SqlTransaction _stageTransaction = null;

//        protected override void OnCommit(Delivery delivery, int pass)
//        {
//            string tablePerfix = delivery.Parameters[Consts.DeliveryHistoryParameters.TablePerfix].ToString();
//            string deliveryId = delivery.DeliveryID.ToString("N");
			
//            // ...........................
//            // COMMIT data to OLTP

//            _stageCommand = _stageCommand ?? DataManager.CreateCommand(Options.SqlStageCommand, CommandType.StoredProcedure);
//            _stageCommand.Connection = _sqlConnection;
//            _stageCommand.Transaction = _stageTransaction;

//            _stageCommand.Parameters["@DeliveryTablePrefix"].Size = 4000;
//            _stageCommand.Parameters["@DeliveryTablePrefix"].Value = tablePerfix;
//            _stageCommand.Parameters["@DeliveryID"].Size = 4000;
//            _stageCommand.Parameters["@DeliveryID"].Value = deliveryId;
			
//            _stageCommand.Parameters["@OutputIDsPerSignature"].Size = 4000;
//            _stageCommand.Parameters["@OutputIDsPerSignature"].Direction = ParameterDirection.Output;

//            _stageCommand.Parameters["@CommitTableName"].Size = 4000;
//            _stageCommand.Parameters["@CommitTableName"].Direction = ParameterDirection.Output;

//            try
//            {
//                _stageCommand.ExecuteNonQuery();

//                string outPutsIDsPerSignature = _stageCommand.Parameters["@OutputIDsPerSignature"].Value.ToString();
//                delivery.Parameters["CommitTableName"] = _stageCommand.Parameters["@CommitTableName"].Value.ToString();

//                string[] existsOutPuts;
//                if ((!string.IsNullOrEmpty(outPutsIDsPerSignature) && outPutsIDsPerSignature != "0"))
//                {
//                    _stageTransaction.Rollback();
//                    existsOutPuts = outPutsIDsPerSignature.Split(',');
//                    List<DeliveryOutput> outputs = new List<DeliveryOutput>();
//                    foreach (string existOutput in existsOutPuts)
//                    {
//                        DeliveryOutput o = DeliveryOutput.Get(Guid.Parse(existOutput));
//                        o.Parameters[Consts.DeliveryHistoryParameters.CommitTableName] = delivery.Parameters["CommitTableName"];
//                        outputs.Add(o);

//                    }
//                    throw new DeliveryConflictException(string.Format("DeliveryOutputs with the same signature are already committed in the database\n Deliveries:\n {0}:", outPutsIDsPerSignature)) { ConflictingOutputs = outputs.ToArray() };
//                }
//                else
//                    //already updated by sp, this is so we don't override it
//                    foreach (var output in delivery.Outputs)
//                    {
//                        output.Status = DeliveryOutputStatus.Staged;
//                    }

//            }
//            finally
//            {
//                this.State = DeliveryImportManagerState.Idle;

//            }



//        }

//        protected override void OnBeginCommit()
//        {
//            if (String.IsNullOrWhiteSpace(this.Options.SqlStageCommand))
//                throw new ConfigurationException("Options.SqlStageCommand is empty.");

//            _sqlConnection = NewDeliveryDbConnection();
//            _sqlConnection.Open();

//            _stageTransaction = _sqlConnection.BeginTransaction("Delivery Commiting");
//        }

//        protected override void OnEndCommit(Exception ex)
//        {
//            if (_stageTransaction != null)
//            {
//                if (ex == null)
//                    _stageTransaction.Commit();
//                else
//                    _stageTransaction.Rollback();
//            }
//            this.State = DeliveryImportManagerState.Idle;
//        }

//        protected override void OnDisposeCommit()
//        {
//            if (_stageTransaction != null)
//                _stageTransaction.Dispose();
//        }

//        #region Rollback
//        /*=========================*/
//        SqlCommand _rollbackCommand = null;
//        SqlTransaction _rollbackTransaction = null;

//        protected override void OnRollbackOutput(DeliveryOutput output, int pass)
//        {
//            string guid = output.OutputID.ToString("N");

//            _rollbackCommand = _rollbackCommand ?? DataManager.CreateCommand(this.Options.SqlRollbackCommand, CommandType.StoredProcedure);
//            _rollbackCommand.Connection = _sqlConnection;
//            _rollbackCommand.Transaction = _rollbackTransaction;

//            _rollbackCommand.Parameters["@DeliveryOutputID"].Value = guid;
//            _rollbackCommand.Parameters["@TableName"].Value = output.Parameters[Consts.DeliveryHistoryParameters.CommitTableName];

//            _rollbackCommand.ExecuteNonQuery();


//            // This is redundant (SP already does this) but to sync our objects in memory we do it here also
//            output.Status = DeliveryOutputStatus.RolledBack;

//        }

//        protected override void OnRollbackDelivery(Delivery delivery, int pass)
//        {
//            string guid = delivery.DeliveryID.ToString("N");

//            _rollbackCommand = _rollbackCommand ?? DataManager.CreateCommand(this.Options.SqlRollbackCommand, CommandType.StoredProcedure);
//            _rollbackCommand.Connection = _sqlConnection;
//            _rollbackCommand.Transaction = _rollbackTransaction;

//            _rollbackCommand.Parameters["@DeliveryID"].Value = guid;
//            _rollbackCommand.Parameters["@TableName"].Value = this.CurrentDelivery.Parameters[Consts.DeliveryHistoryParameters.CommitTableName];

//            _rollbackCommand.ExecuteNonQuery();

//            // This is redundant (SP already does this) but to sync our objects in memory we do it here also
//            foreach (DeliveryOutput output in delivery.Outputs)
//                output.Status = DeliveryOutputStatus.RolledBack;
//        }

//        protected override void OnEndRollback(Exception ex)
//        {
//            if (ex == null)
//                _rollbackTransaction.Commit();
//            else
//                _rollbackTransaction.Rollback();
//        }

//        protected override void OnDisposeRollback()
//        {
//            if (_rollbackTransaction != null)
//                _rollbackTransaction.Dispose();
//        }

//        /*=========================*/
//        #endregion
		
//    }

	
//}
