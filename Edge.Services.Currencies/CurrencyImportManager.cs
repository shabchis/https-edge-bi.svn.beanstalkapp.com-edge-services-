using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Common.Importing;
using System.Data;
using System.Data.SqlClient;
using Edge.Data.Pipeline.Metrics;
using Edge.Core.Configuration;

namespace Edge.Services.Currencies
{
	public class CurrencyImportManager: DeliveryImportManager
	{

		#region Fields
		/*=========================*/

		private SqlConnection _sqlConnection;

		/*=========================*/
		#endregion

		private Dictionary<Type, BulkObjects> _bulks;

		protected override void OnBeginImport()
		{
			this._tablePrefix = string.Format("{0}_{1}_{2}_{3}", this.TablePrefixType, this.CurrentDelivery.Account.ID, DateTime.Now.ToString("yyyMMdd_HHmmss"), this.CurrentDelivery.DeliveryID.ToString("N").ToLower());
			this.CurrentDelivery.Parameters.Add(Consts.DeliveryHistoryParameters.TablePerfix, this._tablePrefix);

			int bufferSize = int.Parse(AppSettings.Get(this, Consts.AppSettings.BufferSize));

			// Connect to database
			_sqlConnection = NewDeliveryDbConnection();
			_sqlConnection.Open();

			// Create bulk objects
			_bulks = new Dictionary<Type, BulkObjects>();
			Type tableList = this.GetType().GetNestedType("Tables", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
			foreach (Type table in tableList.GetNestedTypes())
			{
				_bulks[table] = new BulkObjects(this._tablePrefix, table, _sqlConnection, bufferSize);
			}

			//// Get measures
			//using (SqlConnection oltpConnection = new SqlConnection(this.Options.StagingConnectionString))
			//{
			//    oltpConnection.Open();

			//    this.Measures = Measure.GetMeasures(
			//        this.CurrentDelivery.Account,
			//        this.CurrentDelivery.Channel,
			//        oltpConnection,
			//        this.Options.MeasureOptions,
			//        this.Options.MeasureOptionsOperator
			//        );

			//    this.SegmentTypes = Segment.GetSegments(
			//        this.CurrentDelivery.Account,
			//        this.CurrentDelivery.Channel,
			//        oltpConnection,
			//        this.Options.SegmentOptions,
			//        this.Options.SegmentOptionsOperator
			//        );
			//}

			// Add measure columns to metrics
			//StringBuilder measuresFieldNamesSQL = new StringBuilder(",");
			//StringBuilder measuresNamesSQL = new StringBuilder(",");
			//StringBuilder measuresValidationSQL = new StringBuilder();
			//int count = 0;
			//BulkObjects bulkMetrics = _bulks[this.MetricsTableDefinition];
			//foreach (Measure measure in this.Measures.Values)
			//{
			//    bulkMetrics.AddColumn(new ColumnDef(
			//        name: measure.Name,
			//        type: SqlDbType.Float,
			//        nullable: true
			//        ));

			//    measuresFieldNamesSQL.AppendFormat("[{0}]{1}", measure.OltpName, count < this.Measures.Values.Count - 1 ? "," : null);
			//    measuresNamesSQL.AppendFormat("[{0}]{1}", measure.Name, count < this.Measures.Values.Count - 1 ? "," : null);

			//    if (measure.Options.HasFlag(MeasureOptions.ValidationRequired))
			//        measuresValidationSQL.AppendFormat("{1}SUM([{0}]) as [{0}]", measure.Name, measuresValidationSQL.Length > 0 ? ", " : null);

			//    count++;
			//}

			//this.CurrentDelivery.Parameters.Add(Consts.DeliveryHistoryParameters.MeasureFieldsSql, measuresFieldNamesSQL.ToString());
			//this.CurrentDelivery.Parameters.Add(Consts.DeliveryHistoryParameters.MeasureNamesSql, measuresNamesSQL.ToString());
			//if (string.IsNullOrEmpty(measuresValidationSQL.ToString()))
			//    Log.Write("No measures marked for checksum validation; there will be no validation before the final commit.", LogMessageType.Warning);
			//else
			//    this.CurrentDelivery.Parameters.Add(Consts.DeliveryHistoryParameters.MeasureValidateSql, measuresValidationSQL.ToString());

			// Create the tables
			StringBuilder createTableCmdText = new StringBuilder();
			foreach (BulkObjects bulk in _bulks.Values)
				createTableCmdText.Append(bulk.GetCreateTableSql());
			SqlCommand cmd = new SqlCommand(createTableCmdText.ToString(), _sqlConnection);
			cmd.CommandTimeout = 80; //DEFAULT IS 30 AND SOMTIME NOT ENOUGH WHEN RUNING CUBE
			cmd.ExecuteNonQuery();
		}
		protected override void OnEndImport()
		{
			foreach (BulkObjects bulk in _bulks.Values)
			{
				bulk.Flush();
				bulk.Dispose();
			}
		}

		#region Table
		/*=========================*/
		private string _tablePrefix;

		public static class Tables
		{
			public class Currency
			{
				public static ColumnDef RateSymbol = new ColumnDef("RateSymbol", type: SqlDbType.NVarChar, size: 100, nullable: false);
				public static ColumnDef RateDate = new ColumnDef("RateDate", type: SqlDbType.DateTime, size: 100, nullable: false);
				public static ColumnDef RateValue = new ColumnDef("RateValue", type: SqlDbType.Float,nullable: false);
			}
		}
		protected override string TablePrefixType
		{
			get { return "Currency"; }
		}
		/*=========================*/
		#endregion

		protected override void OnTransform(Delivery delivery, int pass)
		{
			throw new NotImplementedException();
		}

		protected override void OnStage(Delivery delivery, int pass)
		{
			throw new NotImplementedException();
		}

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
