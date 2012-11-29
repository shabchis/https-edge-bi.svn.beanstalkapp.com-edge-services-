using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Data.Pipeline.Services;
using Newtonsoft.Json;
using Edge.Core.Utilities;
using Edge.Core.Data;

namespace Edge.Services.AdMetrics.Validations
{
	class ValidationResultsHandler : Service
	{
		protected sealed override ServiceOutcome DoWork() 
		{
			foreach (string checkInstance in GetChecksumServicesInstaceIdList(this.Instance.ParentInstance.InstanceID,this.Instance.InstanceID))			
			{
				Dictionary<string, List<ValidationResult>> results = GetResultsByInstanceId(Convert.ToInt64(checkInstance));
				if (results[ValidationResultType.Error.ToString()].Count > 0)
				{
					Alert(this.Instance.ParentInstance.Configuration.Options["ProfileName"].Trim(), results[ValidationResultType.Error.ToString()]);
				}
			}

			return ServiceOutcome.Success;
		}

		private void Alert(string topic, List<ValidationResult> results)
		{
			StringBuilder msg = new StringBuilder();
			foreach (ValidationResult item in results)
			{
				msg.AppendLine(string.Format("ID:{0}-Channel:{1}",item.AccountID,item.ChannelID));
			}

			Smtp.SetFromTo(this.Instance.Configuration.Options["AlertFrom"].ToString(), this.Instance.Configuration.Options["AlertTo"].ToString());

			if (!string.IsNullOrEmpty(this.Instance.Configuration.Options["CC"]))
			{
				Smtp.SetCc(this.Instance.Configuration.Options["CC"].ToString());
			}
			
			Smtp.Send(topic+" Data Error:", msg.ToString(), highPriority: true);

		}

		private List<string> GetChecksumServicesInstaceIdList(long parentInstaceId,long currentInstanceId)
		{
			List<string> instances = new List<string>();
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString("Edge.Core.Services", "SystemDatabase")))
			{
				sqlCon.Open();
				SqlCommand sqlCommand = DataManager.CreateCommand(
					"SELECT [InstanceID] FROM [dbo].[ServiceInstance] where [ParentInstanceID] = @ParentInstanceID:int and [InstanceID] != @InstanceId:int ");

				sqlCommand.Parameters["@ParentInstanceID"].Value = parentInstaceId;
				sqlCommand.Parameters["@InstanceId"].Value = currentInstanceId;

				//sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = "@ParentInstanceID", Value = parentInstaceId, SqlDbType = System.Data.SqlDbType.BigInt });
				//sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = "@InstanceId", Value = currentInstanceId, SqlDbType = System.Data.SqlDbType.BigInt });
				sqlCommand.Connection = sqlCon;

				using (var _reader = sqlCommand.ExecuteReader())
				{
					if (!_reader.IsClosed)
					{
						while (_reader.Read())
						{
							instances.Add(_reader[0].ToString());
						}
					}
				}
			}
			
			return instances;
		}

		private Dictionary<string, List<ValidationResult>> GetResultsByInstanceId(long instanceId)
		{

			Dictionary<string, List<ValidationResult>> results = new Dictionary<string, List<ValidationResult>>()
				{
					{ValidationResultType.Error.ToString(),new List<ValidationResult>()},
					{ValidationResultType.Warning.ToString(),new List<ValidationResult>()},
					{ValidationResultType.Information.ToString(),new List<ValidationResult>()}
				};

			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString("Edge.Core.Services", "SystemDatabase")))
			{


				sqlCon.Open();
				SqlCommand sqlCommand = new SqlCommand(
					"SELECT [Message] FROM [dbo].[Log] where [ServiceInstanceID] = @instanceID");

				sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = "@instanceID", Value = instanceId, SqlDbType = System.Data.SqlDbType.BigInt });
				sqlCommand.Connection = sqlCon;

				using (var _reader = sqlCommand.ExecuteReader())
				{
					if (!_reader.IsClosed)
					{
						while (_reader.Read())
						{
							if (_reader[0].ToString().StartsWith("{\"ResultType\""))
							{
								ValidationResult result = (ValidationResult)JsonConvert.DeserializeObject(_reader[0].ToString(), typeof(ValidationResult));
								results[result.ResultType.ToString()].Add(result);
							}
						}
					}
				}
			}
			return results;

		}
	}
}
