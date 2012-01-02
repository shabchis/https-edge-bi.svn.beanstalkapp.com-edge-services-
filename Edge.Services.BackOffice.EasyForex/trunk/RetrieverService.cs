using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Net;
using Edge.Core.Services;

namespace Edge.Services.BackOffice.EasyForex
{
	class RetrieverService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{

			//INT
			this.Delivery = this.NewDelivery();
			this.Delivery.Account = new Account() { ID = this.Instance.AccountID };
			this.Delivery.TargetPeriod = this.TargetPeriod;
			this.Delivery.Channel = new Channel() { ID = -1 };
			this.Delivery.TargetLocationDirectory = @"D:\";
			this.Delivery.Signature = Delivery.CreateSignature(String.Format("BackOffice-[{0}]-[{1}]", this.Instance.AccountID, this.TargetPeriod.ToAbsolute()));

			DeliveryFile _file = new DeliveryFile()
			{
				Account = this.Delivery.Account,
				Name = "EasyForexBackOffice",
				SourceUrl = "https://classic.easy-forex.com/BackOffice/API/Marketing.asmx",
			};

			_file.Parameters.Add("SoapAction", "http://www.easy-forex.com/GetCampaignStatisticsNEW");
			_file.Parameters.Add("Content-Type", "text/xml; charset=utf-8");

			//Creating  Soap Body
			string strSoapEnvelope = string.Empty;
			strSoapEnvelope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
			strSoapEnvelope += " <soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">";
			strSoapEnvelope += "  <soap:Header>";
			strSoapEnvelope += " <AuthHeader xmlns=\"http://www.easy-forex.com\">";
			strSoapEnvelope += "  <Username>Seperia</Username>";
			strSoapEnvelope += "  <Password>Seperia909</Password>";
			strSoapEnvelope += "  </AuthHeader>";
			strSoapEnvelope += " </soap:Header>";
			strSoapEnvelope += " <soap:Body>";
			strSoapEnvelope += " <GetCampaignStatisticsNEW xmlns=\"http://www.easy-forex.com\">";
			strSoapEnvelope += " <startGid>1</startGid>";
			strSoapEnvelope += " <finishGid>1000000</finishGid>";
			strSoapEnvelope += " <fromDateTime>dateTime</fromDateTime>";
			strSoapEnvelope += " <toDateTime>dateTime</toDateTime>";
			strSoapEnvelope += " </GetCampaignStatisticsNEW>";
			strSoapEnvelope += " </soap:Body>";
			strSoapEnvelope += " </soap:Envelope>";

			_file.Parameters.Add("Body", strSoapEnvelope);

			this.Delivery.Save();

			/* INT End */

			/* SoapRetriever */

			// Create a batch and use its progress as the service's progress
			BatchDownloadOperation batch = new BatchDownloadOperation();
			batch.Progressed += new EventHandler((sender, e) =>
			{
				this.ReportProgress(batch.Progress * 0.95);
			});

			foreach (DeliveryFile file in this.Delivery.Files)
			{
				if (String.IsNullOrWhiteSpace(file.SourceUrl))
					continue;

				WebRequest request = WebRequest.Create(file.SourceUrl);
				request.ContentType = file.Parameters["Content-Type"].ToString();


				byte[] bytes = Encoding.UTF8.GetBytes(file.Parameters["Body"].ToString());
				request.ContentLength = bytes.Length;

				request.Headers = new WebHeaderCollection();
				using (var stream = request.GetRequestStream())
				{
					
					stream.Write(bytes,0,bytes.Length);
				}

				//Headers

				request.Headers.Add("SoapAction", file.Parameters["SoapAction"].ToString());

				//foreach (var per in file.Parameters)
				//{
				//    if (per.Key.StartsWith("Header"))
				//        request.Headers.Add(per.Value.ToString());
				//}

				DeliveryFileDownloadOperation download = file.Download(request);
				download.Ended += new EventHandler(download_Ended);
				batch.Add(download);
			}

			batch.Start();
			batch.Wait();

			// Add a retrieved history entry for the entire delivery
			this.Delivery.History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
			this.Delivery.Save();

			return ServiceOutcome.Success;
		}

		void download_Ended(object sender, EventArgs e)
		{
			// Add a retrieved history entry to every file
			((DeliveryFileDownloadOperation)sender).DeliveryFile.History.Add(DeliveryOperation.Retrieved, this.Instance.InstanceID);
		}
	}
}
