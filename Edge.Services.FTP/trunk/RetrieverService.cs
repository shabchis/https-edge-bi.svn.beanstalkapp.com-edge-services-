using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;
using System.IO;

namespace Edge.Services.FTP
{
	class RetrieverService : PipelineService
	{
		private BatchDownloadOperation _batchDownloadOperation;
		private int _filesInProgress = 0;
		private double _minProgress = 0.05;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			if (Delivery.Files!=null && Delivery.Files.Count>0)
			{
				_batchDownloadOperation = new BatchDownloadOperation();
				_batchDownloadOperation.Progressed += new EventHandler(_batchDownloadOperation_Progressed);

				_filesInProgress = this.Delivery.Files.Count;

				foreach (DeliveryFile file in this.Delivery.Files)
				{
					DownloadFile(file);
				}

				_batchDownloadOperation.Start();
				_batchDownloadOperation.Wait();
				_batchDownloadOperation.EnsureSuccess();
				this.Delivery.Save(); 
			}

			

			
			return Core.Services.ServiceOutcome.Success;
		}
		void _batchDownloadOperation_Progressed(object sender, EventArgs e)
		{
			BatchDownloadOperation DownloadOperation = (BatchDownloadOperation)sender;
			this.ReportProgress(DownloadOperation.Progress);
		}
		private void DownloadFile(DeliveryFile file)
		{
			FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(file.SourceUrl);
			request.UseBinary = true;
			request.Credentials = new NetworkCredential
				(
					this.Delivery.Parameters["UserID"].ToString(),
					this.Delivery.Parameters["Password"].ToString()
				);
			request.Method = WebRequestMethods.Ftp.DownloadFile;
			request.UsePassive = true;

			_batchDownloadOperation.Add(file.Download(request, Convert.ToInt64(file.Parameters["Size"])));

		}
	}
}
