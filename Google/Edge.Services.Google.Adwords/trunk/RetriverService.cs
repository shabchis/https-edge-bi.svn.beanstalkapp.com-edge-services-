using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using System.Net;

namespace Edge.Services.Google.Adwords
{
	class RetriverService : PipelineService
	{
		#region consts
		public const string FileRelativePath = "FileRelativePath";
		public const string returnMonetInMicros_Header = "returnMoneyInMicros";
		public const string autorization_Header = "Authorization";
		public const string clientEmail_Header = "clientEmail";
		public const string clientCustomerId_Header = "clientCustomerId";
		#endregion
		#region members
		private int _countedFile = 0;
		private double _minProgress = 0.05;
		#endregion
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			_countedFile = this.Delivery.Files.Count;
			foreach ( DeliveryFile file in this.Delivery.Files)
			{
				DownloadFile(file);
				
			}
			return Core.Services.ServiceOutcome.Success;
		}

		private void DownloadFile(DeliveryFile file)
		{
					
			//string body = file.Parameters["body"].ToString();
			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(file.Parameters["Url"].ToString());
			if (file.Parameters.ContainsKey(clientCustomerId_Header))
				request.Headers.Add("clientCustomerId: " + file.Parameters[clientCustomerId_Header].ToString());
			else if (file.Parameters.ContainsKey(clientEmail_Header))
				request.Headers.Add("clientEmail: " + file.Parameters[clientEmail_Header].ToString());

			if (file.Parameters.ContainsKey(autorization_Header))
				request.Headers.Add("Authorization: GoogleLogin auth=" + file.Parameters[autorization_Header].ToString());

			if (file.Parameters.ContainsKey(returnMonetInMicros_Header))
				request.Headers.Add("returnMoneyInMicros: " + file.Parameters[returnMonetInMicros_Header].ToString());

			WebResponse response = request.GetResponse();

			FileDownloadOperation fileDownloadOperation = FileManager.Download(response.GetResponseStream(), file.Parameters[FileRelativePath].ToString(), true, response.ContentLength);
			fileDownloadOperation.Progressed += new EventHandler<ProgressEventArgs>(fileDownloadOperation_Progressed);
			fileDownloadOperation.Ended += new EventHandler<EndedEventArgs>(fileDownloadOperation_Ended);
			fileDownloadOperation.Start();
		}

		void fileDownloadOperation_Ended(object sender, EndedEventArgs e)
		{
			_countedFile -= 1;
		}

		void fileDownloadOperation_Progressed(object sender, ProgressEventArgs e)
		{
			double percent = Math.Round(Convert.ToDouble(Convert.ToDouble(e.DownloadedBytes) / Convert.ToDouble(e.TotalBytes) / (double)_countedFile), 3);
			if (percent >= _minProgress)
			{
				_minProgress += 0.05;
				if (percent <= 1)
					this.ReportProgress(percent); 
			}
		}

		
	}
}
