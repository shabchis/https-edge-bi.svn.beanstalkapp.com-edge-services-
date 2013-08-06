using System;
using Edge.Data.Objects;
using Edge.Data.Pipeline.Metrics.Managers;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Core.Services;

namespace Edge.Services.BackOffice.EasyForex
{
	public class InitializerService : PipelineService
	{
		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			if (!Configuration.TimePeriod.HasValue)
				throw new Exception("No time period is set for Service");

			if (!Configuration.Parameters.ContainsKey("AccountID"))
				throw new Exception("Missing Configuration Param: AccountID");

			var accountId = Configuration.Parameters.Get("AccountID", false, -1);
			var channelId = Configuration.Parameters.Get("ChannelID", false, -1);

			// create delivery
			if (Delivery == null)
			{
				Delivery = NewDelivery();
				Delivery.TimePeriodDefinition = Configuration.TimePeriod.Value;
				Delivery.Account = new Account { ID = accountId };
				Delivery.FileDirectory = Configuration.Parameters.Get<string>(Const.DeliveryServiceConfigurationOptions.FileDirectory);
				if (channelId != -1) Delivery.Channel = new Channel { ID = channelId };
			}

			if (string.IsNullOrEmpty(Delivery.FileDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");

			// delivery output
			Delivery.Outputs.Add(new DeliveryOutput
			{
				Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]",
					Delivery.Account.ID,
					Delivery.TimePeriodDefinition.ToAbsolute())),
				Account = Delivery.Account,
				Channel = Delivery.Channel,
				TimePeriodStart = Delivery.TimePeriodStart,
				TimePeriodEnd = Delivery.TimePeriodEnd
			});

			Progress = 0.2;

			// Create an import manager that will handle rollback, if necessary
			HandleConflicts(new MetricsDeliveryManager(InstanceID), DeliveryConflictBehavior.Abort);

			Progress = 0.5;

			//Create File
			var file = new DeliveryFile
				{
					Name = "EasyForexBackOffice",
					SourceUrl = Configuration.Parameters.Get<string>("SourceUrl")
				};

			//_file.Parameters.Add("SOAPAction", "http://www.easy-forex.com/GetGatewayStatistics");
			file.Parameters.Add("Content-Type", "text/xml; charset=utf-8");
			file.Parameters.Add("SOAPAction", Configuration.Parameters["SOAPAction"]);

			Delivery.Parameters.Add("User", Configuration.Parameters["User"]);
			Delivery.Parameters.Add("Pass", Core.Utilities.Encryptor.Dec(Configuration.Parameters["Pass"].ToString()));
			Delivery.Parameters.Add("SoapMethod", Configuration.Parameters["SoapMethod"]);
			Delivery.Parameters.Add("StartGid", Configuration.Parameters["StartGid"]);
			Delivery.Parameters.Add("EndGid", Configuration.Parameters["EndGid"]);

			//Creating  Soap Body
			var strSoapEnvelope = GetSoapEnvelope(
				 Delivery.Parameters["User"].ToString(),
				 Delivery.Parameters["Pass"].ToString(),
				 Delivery.Parameters["SoapMethod"].ToString(),
				 Delivery.Parameters["StartGid"].ToString(),
				 Delivery.Parameters["EndGid"].ToString(),
				 Delivery.TimePeriodStart.ToString("yyyy-MM-ddTHH:mm:ss"),
				 Delivery.TimePeriodEnd.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")
				 );

			file.Parameters.Add("Body", strSoapEnvelope);
			file.Parameters.Add("Bo.IsAttribute", Configuration.Parameters["Bo.IsAttribute"]);
			file.Parameters.Add("Bo.Xpath", Configuration.Parameters["Bo.Xpath"]);
			file.Parameters.Add("Bo.TrackerIDField", Configuration.Parameters["Bo.TrackerIDField"]);

			Delivery.Files.Add(file);

			Progress = 0.8;

			// Save with success
			Delivery.Save();
			return ServiceOutcome.Success;
		} 
		#endregion

		#region Private Methods
		private string GetSoapEnvelope(string user, string pass, string methodName, string startGid, string endGid, string fromDate, string toDate)
		{
			//Soap 1.1
			#region EnvelopeFormat

			var strSoapEnvelope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
			strSoapEnvelope += "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">";
			strSoapEnvelope += "<soap:Header>";
			strSoapEnvelope += "<AuthHeader xmlns=\"http://www.easy-forex.com\">";
			strSoapEnvelope += "<Username>{USER}</Username>";
			strSoapEnvelope += "<Password>{PASS}</Password>";
			strSoapEnvelope += "</AuthHeader>";
			strSoapEnvelope += "</soap:Header>";
			strSoapEnvelope += "<soap:Body>";
			strSoapEnvelope += "<{METHOD} xmlns=\"http://www.easy-forex.com\">";
			strSoapEnvelope += "<startGid>{START_GID}</startGid>";
			strSoapEnvelope += "<finishGid>{END_GID}</finishGid>";
			strSoapEnvelope += "<fromDateTime>{FROM_DATE}</fromDateTime>";
			strSoapEnvelope += "<toDateTime>{TO_DATE}</toDateTime>";
			strSoapEnvelope += " </{METHOD}>";
			strSoapEnvelope += " </soap:Body>";
			strSoapEnvelope += " </soap:Envelope>";

			#endregion

			strSoapEnvelope = strSoapEnvelope.Replace("{USER}", user);
			strSoapEnvelope = strSoapEnvelope.Replace("{PASS}", pass);
			strSoapEnvelope = strSoapEnvelope.Replace("{METHOD}", methodName);
			strSoapEnvelope = strSoapEnvelope.Replace("{START_GID}", startGid);
			strSoapEnvelope = strSoapEnvelope.Replace("{END_GID}", endGid);
			strSoapEnvelope = strSoapEnvelope.Replace("{FROM_DATE}", fromDate);
			strSoapEnvelope = strSoapEnvelope.Replace("{TO_DATE}", toDate);

			return strSoapEnvelope;
		} 
		#endregion
	}
}
