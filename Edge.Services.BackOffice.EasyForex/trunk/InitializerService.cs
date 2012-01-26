using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Services.SegmentMetrics;
using Edge.Core.Services;

namespace Edge.Services.BackOffice.EasyForex
{
	class InitializerService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			#region Init General
			// ...............................
			// SETUP
			this.Delivery = this.NewDelivery();

			// This is for finding conflicting services
			this.Delivery.Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]", this.Instance.AccountID, this.TargetPeriod.ToAbsolute()));

            /*
			// Create an import manager that will handle rollback, if necessary
			SegmentMetricsImportManager importManager = new SegmentMetricsImportManager(this.Instance.InstanceID, new Edge.Data.Pipeline.Common.Importing.ImportManagerOptions()
			{
				SqlRollbackCommand = Instance.Configuration.Options[Edge.Data.Pipeline.Common.Importing.Consts.AppSettings.SqlRollbackCommand]
			});

			// Apply the delivery (will use ConflictBehavior configuration option to abort or rollback if any conflicts occur)
			this.HandleConflicts(importManager, DeliveryConflictBehavior.Abort);

			// ...............................
            */
			// Now that we have a new delivery, start adding values
			this.Delivery.Account = new Data.Objects.Account()
			{
				ID = this.Instance.AccountID,
			};
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Data.Objects.Channel()
			{
				ID = -1
			};

			this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];

			if (string.IsNullOrEmpty(this.Delivery.TargetLocationDirectory))
				throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");
            
            this.ReportProgress(0.2);
			#endregion

           //Create File

            DeliveryFile _file = new DeliveryFile()
            {
                Account = this.Delivery.Account,
                Name = "EasyForexBackOffice",
                
            };

			_file.SourceUrl = Instance.Configuration.Options["SourceUrl"];// "https://classic.easy-forex.com/BackOffice/API/Marketing.asmx";
            //_file.Parameters.Add("SOAPAction", "http://www.easy-forex.com/GetGatewayStatistics");
            _file.Parameters.Add("Content-Type", "text/xml; charset=utf-8");
            _file.Parameters.Add("SOAPAction",Instance.Configuration.Options["SOAPAction"]);

            Delivery.Parameters.Add("User",Instance.Configuration.Options["User"]);
            Delivery.Parameters.Add("Pass",Instance.Configuration.Options["Pass"]);
            Delivery.Parameters.Add("SoapMethod",Instance.Configuration.Options["SoapMethod"]);
            Delivery.Parameters.Add("StartGid",Instance.Configuration.Options["StartGid"]);
            Delivery.Parameters.Add("EndGid",Instance.Configuration.Options["EndGid"]);


            //Creating  Soap Body
            string strSoapEnvelope = GetSoapEnvelope(
                 Delivery.Parameters["User"].ToString(),
                 Delivery.Parameters["Pass"].ToString(),
                 Delivery.Parameters["SoapMethod"].ToString(),
                 Delivery.Parameters["StartGid"].ToString(),
                 Delivery.Parameters["EndGid"].ToString(),
                 this.Delivery.TargetPeriodStart.ToString("yyyy-MM-ddTHH:mm:ss"),
                 this.Delivery.TargetPeriodEnd.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")
                 );
           
            #region Soap 1.2
		 //strSoapEnvelope += "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
            //strSoapEnvelope += "<soap12:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap12=\"http://www.w3.org/2003/05/soap-envelope\">";
            //strSoapEnvelope += " <soap12:Header>";
            //strSoapEnvelope += " <AuthHeader xmlns=\"http://www.easy-forex.com\">";
            //strSoapEnvelope += "  <Username>Seperia</Username>";
            //strSoapEnvelope += "  <Password>Seperia909</Password>";
            //strSoapEnvelope += "  </AuthHeader>";
            //strSoapEnvelope += " </soap12:Header>";
            //strSoapEnvelope += " <soap12:Body>";
            //strSoapEnvelope += "<GetGatewayStatistics xmlns=\"http://www.easy-forex.com\">";
            //strSoapEnvelope += " <startGid>1</startGid>";
            //strSoapEnvelope += " <finishGid>1000000</finishGid>";
            //strSoapEnvelope += string.Format(" <fromDateTime>{0}</fromDateTime>", _requiredDay);
            //strSoapEnvelope += string.Format(" <toDateTime>{0}</toDateTime>", _requiredDay.AddDays(1).AddTicks(-1));
            //strSoapEnvelope += "  </GetGatewayStatistics>";
            //strSoapEnvelope += "</soap12:Body>";
            //strSoapEnvelope += "</soap12:Envelope>";
	#endregion
            _file.Parameters.Add("Body", strSoapEnvelope);
            _file.Parameters.Add("Bo.IsAttribute", Instance.Configuration.Options["Bo.IsAttribute"]);
            _file.Parameters.Add("Bo.Xpath",Instance.Configuration.Options["Bo.Xpath"]);
            _file.Parameters.Add("Bo.TrackerIDField",Instance.Configuration.Options["Bo.TrackerIDField"]);

            this.Delivery.Files.Add(_file);
        
			// Save with success
			this.Delivery.Save();

			return ServiceOutcome.Success;
		
		}

        private string GetSoapEnvelope(string user , string pass, string methodName,string startGid, string endGid, string fromDate , string toDate)
        {
            //Soap 1.1
            #region EnvelopeFormat
            /*----------------------------------------------------------------*/
		    string strSoapEnvelope = string.Empty;
            strSoapEnvelope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
            strSoapEnvelope += "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">";
            strSoapEnvelope += "<soap:Header>";
            strSoapEnvelope += "<AuthHeader xmlns=\"http://www.easy-forex.com\">";
            //strSoapEnvelope += "<Username>Seperia</Username>";
            strSoapEnvelope += "<Username>{USER}</Username>";
            //strSoapEnvelope += "<Password>Seperia909</Password>";
            strSoapEnvelope += "<Password>{PASS}</Password>";
            strSoapEnvelope += "</AuthHeader>";
            strSoapEnvelope += "</soap:Header>";
            strSoapEnvelope += "<soap:Body>";
            //strSoapEnvelope += "<GetGatewayStatistics xmlns=\"http://www.easy-forex.com\">";
            strSoapEnvelope += "<{METHOD} xmlns=\"http://www.easy-forex.com\">";
            strSoapEnvelope += "<startGid>{START_GID}</startGid>";
            strSoapEnvelope += "<finishGid>{END_GID}</finishGid>";
            //strSoapEnvelope += string.Format("<fromDateTime>{0}</fromDateTime>", _requiredDay.ToString("yyyy-MM-ddTHH:mm:ss"));
            strSoapEnvelope += "<fromDateTime>{FROM_DATE}</fromDateTime>";
            //strSoapEnvelope += string.Format("<toDateTime>{0}</toDateTime>", _requiredDay.AddDays(1).AddTicks(-1).ToString("yyyy-MM-ddTHH:mm:ss.fffffff"));
            strSoapEnvelope += "<toDateTime>{TO_DATE}</toDateTime>";
            //strSoapEnvelope += " </GetGatewayStatistics>";
            strSoapEnvelope += " </{METHOD}>";
            strSoapEnvelope += " </soap:Body>";
            strSoapEnvelope += " </soap:Envelope>";
	        #endregion
            /*-----------------------------------------------------------------*/
            strSoapEnvelope = strSoapEnvelope.Replace("{USER}", user);
            strSoapEnvelope = strSoapEnvelope.Replace("{PASS}", pass);
            strSoapEnvelope = strSoapEnvelope.Replace("{METHOD}", methodName);
            strSoapEnvelope = strSoapEnvelope.Replace("{START_GID}", startGid);
            strSoapEnvelope = strSoapEnvelope.Replace("{END_GID}", endGid);
            strSoapEnvelope = strSoapEnvelope.Replace("{FROM_DATE}", fromDate);
            strSoapEnvelope = strSoapEnvelope.Replace("{TO_DATE}", toDate);

            return strSoapEnvelope;
        }
	}
}
