using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using Edge.Core.Services;

namespace Edge.Services.Currencies
{
	public class InitializerService : PipelineService
	{

		protected override ServiceOutcome DoPipelineWork()
		{
           
			this.Delivery = this.NewDelivery(); // setup delivery
			this.Delivery.FileDirectory = Instance.Configuration.Options[Edge.Data.Pipeline.Services.Const.DeliveryServiceConfigurationOptions.FileDirectory];
			this.Delivery.Account = new Account()
			{
				ID = 0
			};

			

			string fileName = string.Empty;

            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["YahhoApiURL"]))
                this.Delivery.Parameters.Add("YahhoApiURL", this.Instance.Configuration.Options["YahhoApiURL"]);

			//FileName
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["FileName"]))
				throw new Exception("Missing Configuration Param , FileName");
			else
				fileName = this.Instance.Configuration.Options["FileName"];

			//CrossRateSymbols
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["CrossRateSymbols"]))
				throw new Exception("Missing Configuration Param , CrossRateSymbols");
			else
				this.Delivery.Parameters.Add("CrossRateSymbols", this.Instance.Configuration.Options["CrossRateSymbols"]);

			//UserName
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["UserName"]))
				throw new Exception("Missing Configuration Param , UserName");
			else
				this.Delivery.Parameters.Add("UserName", this.Instance.Configuration.Options["UserName"]);

			//UserPassword
			if (String.IsNullOrEmpty(this.Instance.Configuration.Options["UserPassword"]))
				throw new Exception("Missing Configuration Param , UserPassword");
			else
				this.Delivery.Parameters.Add("UserPassword", this.Instance.Configuration.Options["UserPassword"]);


			DeliveryFile _file = new DeliveryFile()
			{
				Name = fileName
			};

			_file.SourceUrl = Instance.Configuration.Options["SourceUrl"];// "http://www.xignite.com/xCurrencies.asmx";
			_file.Parameters.Add("Content-Type", "text/xml; charset=utf-8");
			_file.Parameters.Add("SOAPAction", Instance.Configuration.Options["SOAPAction"]);
			_file.Parameters.Add("SoapMethod", Instance.Configuration.Options["SoapMethod"]);
			_file.Parameters.Add("Body", GetSoapEnvelope(
				this.Instance.Configuration.Options["UserName"].ToString(),
				this.Instance.Configuration.Options["UserPassword"].ToString(),
				this.Instance.Configuration.Options["SoapMethod"].ToString(),
				this.Instance.Configuration.Options["CrossRateSymbols"].ToString(),
				string.Empty,
				this.Delivery.TimePeriodStart.ToString("MM/dd/yyyy")
				));

			_file.Parameters.Add("XML.IsAttribute", Instance.Configuration.Options["XML.IsAttribute"]);
			_file.Parameters.Add("XML.Path", Instance.Configuration.Options["XML.Path"]);

			this.Delivery.Files.Add(_file);


			//Set Output

			foreach (string crossRate in this.Delivery.Parameters["CrossRateSymbols"].ToString().Split(','))
			{
				this.Delivery.Outputs.Add(new DeliveryOutput()
				{
					Signature = Delivery.CreateSignature(String.Format("[{0}]-[{1}]",
						this.TimePeriod.ToAbsolute(),
						crossRate
					)),
					Account = new Data.Objects.Account() { ID = 0 },
					TimePeriodStart = Delivery.TimePeriodStart,
					TimePeriodEnd = Delivery.TimePeriodEnd
				}
			);
			}

			// Create an import manager that will handle rollback, if necessary
			//CurrencyImportManager importManager = new CurrencyImportManager(this.Instance.InstanceID,null);
			//TO DO: Add rollback
			

			// will use ConflictBehavior configuration option to abort or rollback if any conflicts occur
			//this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);


			this.Delivery.Save();
			return Core.Services.ServiceOutcome.Success;
		}


		private string GetSoapEnvelope(string user, string pass, string methodName, string symbols, string tracer, string date)
		{
			#region EnvelopeFormat
			/*----------------------------------------------------------------*/
			string strSoapEnvelope = string.Empty;

			strSoapEnvelope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
			strSoapEnvelope += "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">";

			strSoapEnvelope += "<soap:Header>";
			strSoapEnvelope += "<Header xmlns=\"http://www.xignite.com/services/\">";
			strSoapEnvelope += "<Username>{USER}</Username>";
			strSoapEnvelope += "<Password>{PASS}</Password>";
			strSoapEnvelope += "<Tracer>{TRACER}</Tracer>";
			strSoapEnvelope += "</Header>";
			strSoapEnvelope += "</soap:Header>";

			strSoapEnvelope += "<soap:Body>";
			strSoapEnvelope += "<{METHOD} xmlns=\"http://www.xignite.com/services/\">";
			strSoapEnvelope += "<Symbols>{SYMBOLS}</Symbols>";
			strSoapEnvelope += "<AsOfDate>{DATE}</AsOfDate>";
			strSoapEnvelope += " </{METHOD}>";
			strSoapEnvelope += " </soap:Body>";
			strSoapEnvelope += " </soap:Envelope>";
			#endregion
			/*-----------------------------------------------------------------*/
			strSoapEnvelope = strSoapEnvelope.Replace("{USER}", user);
			strSoapEnvelope = strSoapEnvelope.Replace("{PASS}", pass);
			strSoapEnvelope = strSoapEnvelope.Replace("{METHOD}", methodName);
			strSoapEnvelope = strSoapEnvelope.Replace("{SYMBOLS}", symbols);
			strSoapEnvelope = strSoapEnvelope.Replace("{DATE}", date);
			strSoapEnvelope = strSoapEnvelope.Replace("{TRACER}", tracer);


			return strSoapEnvelope;
		}
	}
}
