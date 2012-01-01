using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;

namespace Edge.Services.BackOffice.EasyForex
{
	class RetrieverService : PipelineService
	{


		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			string strSoapEnvelope = string.Empty;
			strSoapEnvelope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
			strSoapEnvelope += " <soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">";
			strSoapEnvelope += "  <soap:Header>";
			strSoapEnvelope += " <AuthHeader xmlns=\"http://www.easy-forex.com\">";
			strSoapEnvelope += "  <Username>string</Username>";
			strSoapEnvelope += "  <Password>string</Password>";
			strSoapEnvelope += "  </AuthHeader>";
			strSoapEnvelope += " </soap:Header>";
			strSoapEnvelope += " <soap:Body>";
			strSoapEnvelope += " <GetCampaignStatisticsNEW xmlns=\"http://www.easy-forex.com\">";
			strSoapEnvelope += " <startGid>int</startGid>";
			strSoapEnvelope += " <finishGid>int</finishGid>";
			strSoapEnvelope += " <fromDateTime>dateTime</fromDateTime>";
			strSoapEnvelope += " <toDateTime>dateTime</toDateTime>";
			strSoapEnvelope += " </GetCampaignStatisticsNEW>";
			strSoapEnvelope += " </soap:Body>";
			strSoapEnvelope += " </soap:Envelope>";

			//objXMLHttp = new ServerXMLHTTP40();

			throw new NotImplementedException();
		}
	}
}
