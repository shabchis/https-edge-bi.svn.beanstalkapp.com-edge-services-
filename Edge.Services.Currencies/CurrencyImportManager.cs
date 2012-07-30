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
using Edge.Data.Objects;

namespace Edge.Services.Currencies
{
	public class CurrencyImportManager: DeliveryImportManager
	{



		#region Fields
		/*=========================*/

		private SqlConnection _sqlConnection;

		/*=========================*/
		#endregion

		#region Table
		/*=========================*/
		private string _tablePrefix;

		public static class Tables
		{
			public class Currency
			{
				public static ColumnDef RateSymbol = new ColumnDef("RateSymbol", type: SqlDbType.NVarChar, size: 100, nullable: false);
				public static ColumnDef RateDate = new ColumnDef("RateDate", type: SqlDbType.DateTime, size: 100, nullable: false);
				public static ColumnDef RateValue = new ColumnDef("RateValue", type: SqlDbType.Float, nullable: false);
			}
		}
		protected string TablePrefixType
		{
			get { return "Currency"; }
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

		#region Import
		/*=========================*/
		private Dictionary<Type, BulkObjects> _bulks;
		protected BulkObjects Bulk<TableDef>()
		{
			return _bulks[typeof(TableDef)];
		}

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
		public void ImportCurrency(CurrencyRate currencyRate)
		{
			EnsureBeginImport();

			var currencyRow = new Dictionary<ColumnDef, object>()
			{
				{Tables.Currency.RateSymbol,currencyRate.Currency.Code},
				{Tables.Currency.RateValue,currencyRate.RateValue},
				{Tables.Currency.RateDate,currencyRate.RateDate}
			};


			Bulk<Tables.Currency>().SubmitRow(currencyRow);

		}
		protected void EnsureBeginImport()
		{
			if (this.State != DeliveryImportManagerState.Importing)
				throw new InvalidOperationException("BeginImport must be called before anything can be imported.");
		}

		/*=========================*/
		#endregion

		public CurrencyImportManager(long serviceInstanceID)
			: base(serviceInstanceID)
		{
			
		}

		protected override void OnTransform(Delivery delivery, int pass)
		{
			throw new NotImplementedException();
		}
		protected override void OnStage(Delivery delivery, int pass)
		{
			throw new NotImplementedException();
		}

		
	}

	
}
