using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using System.Data.SqlClient;

namespace Edge.Services.SqlServer
{
	public class RunStoredProcedure : Service
	{
		protected override ServiceOutcome DoWork()
		{
			using (SqlConnection sqlConnection=new SqlConnection(this.Instance.Configuration.Options["ConnectionString"]))
			{
				using (SqlCommand sqlCommand=new SqlCommand())
				{
					sqlCommand.Connection = sqlConnection;
					sqlCommand.CommandText = this.Instance.Configuration.Options["StoredProcedureName"];
					sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
					foreach (var option in this.Instance.Configuration.Options)
					{
						if (option.Key.StartsWith("Param"))
						{
							sqlCommand.Parameters.AddWithValue(option.Key.Replace("Param","@"), option.Value);
							
						}
					}
					sqlConnection.Open();
					sqlCommand.ExecuteNonQuery();
					return ServiceOutcome.Success;
					
				}
			}
		}
	}
}
