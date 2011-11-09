﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using System.Data.SqlClient;
using Edge.Core.Configuration;
using Edge.Data.Pipeline.Services;
using Newtonsoft.Json;
using Edge.Core.Utilities;

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
					Alert(results[ValidationResultType.Error.ToString()]);
				}

				//Alert(results[ValidationResultType.Information.ToString()]);
			}

			return ServiceOutcome.Success;
		}

		private void Alert(List<ValidationResult> results)
		{
			StringBuilder msg = new StringBuilder();
			msg.AppendLine("Errors have been found while running CheckSum Service on the following accounts:");
			foreach (ValidationResult item in results)
			{
				msg.AppendLine(string.Format("Account: {0} | Channel: {1} | Message: {2}",item.AccountID,item.ChannelID,item.Message));
			}

			Smtp.SetFromTo(this.Instance.Configuration.Options["AlertFrom"].ToString(), this.Instance.Configuration.Options["AlertTo"].ToString());
			Smtp.Send("Validation Error !!!",msg.ToString(),highPriority: true);

		}

		private List<string> GetChecksumServicesInstaceIdList(long parentInstaceId,long currentInstanceId)
		{
			List<string> instances = new List<string>();
			using (SqlConnection sqlCon = new SqlConnection(AppSettings.GetConnectionString("Edge.Core.Services", "SystemDatabase")))
			{
				sqlCon.Open();
				SqlCommand sqlCommand = new SqlCommand(
					"SELECT [InstanceID] FROM [dbo].[ServiceInstance] where [ParentInstanceID] = @ParentInstanceID and [InstanceID] != @InstanceId ");

				sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = "@ParentInstanceID", Value = parentInstaceId, SqlDbType = System.Data.SqlDbType.BigInt });
				sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = "@InstanceId", Value = currentInstanceId, SqlDbType = System.Data.SqlDbType.BigInt });
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
							ValidationResult result = (ValidationResult)JsonConvert.DeserializeObject(_reader[0].ToString(), typeof(ValidationResult));
							results[result.ResultType.ToString()].Add(result);
						}
					}
				}
			}
			return results;

		}
	}
}
