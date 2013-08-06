using System;
using System.Data;
using System.Data.SqlClient;
using Edge.Core.Configuration;

namespace Edge.Services.SalesForce
{
	public class Token
	{
		#region Data Members
		public string ID { get; set; }
		public string IssuedAt { get; set; }
		public DateTime UpdateTime { get; set; }
		public string RefreshToken { get; set; }
		public string InstanceUrl { get; set; }
		public string Signature { get; set; }
		public string AccessToken { get; set; }
		public string ClientID { get; set; } 
		#endregion

		#region Internal Methods
		internal void Save(string clientID)
		{
			var tokenResponse = new Token();

			using (var connection = new SqlConnection(AppSettings.GetConnectionString(tokenResponse, "DB")))
			{
				connection.Open();
				using (var command = new SqlCommand(AppSettings.Get(typeof(Token), "SP_Save"), connection))
				{
					command.CommandType = CommandType.StoredProcedure;
					command.Parameters.AddWithValue("@Id", ID);
					command.Parameters.AddWithValue("@ClientID", clientID);
					command.Parameters.AddWithValue("@Instance_url", InstanceUrl);
					command.Parameters.AddWithValue("@AccessToken", AccessToken);
					command.Parameters.AddWithValue("@RefreshToken", RefreshToken);
					command.Parameters.AddWithValue("@Signature", Signature);
					command.Parameters.AddWithValue("@Issued_at", IssuedAt);
					command.Parameters.AddWithValue("@UpdateTime", DateTime.Now);

					command.ExecuteNonQuery();
				}
			}
		}
		internal static Token Get(string clientID)
		{
			var tokenResponse = new Token();
			using (var connection = new SqlConnection(AppSettings.GetConnectionString(tokenResponse, "DB")))
			{
				connection.Open();
				using (var command = new SqlCommand(AppSettings.Get(typeof(Token), "SP_Get"), connection))
				{
					command.CommandType = CommandType.StoredProcedure;
					command.Parameters.AddWithValue("@ClientID", clientID);
					using (var reader = command.ExecuteReader())
					{
						if (reader.HasRows)
						{
							reader.Read();
							tokenResponse.UpdateTime = Convert.ToDateTime(reader["UpdateTime"]);
							tokenResponse.ClientID = clientID;
							tokenResponse.ID = reader["Id"].ToString();
							tokenResponse.AccessToken = reader["AccessToken"].ToString();
							tokenResponse.IssuedAt = reader["Issued_at"].ToString();
							tokenResponse.InstanceUrl = reader["Instance_url"].ToString();
							tokenResponse.Signature = reader["Signature"].ToString();
							tokenResponse.RefreshToken = reader["RefreshToken"].ToString();
						}
					}
				}
			}
			return tokenResponse;
		} 
		#endregion
	}
}
