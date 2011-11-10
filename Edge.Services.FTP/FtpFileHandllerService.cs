﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using System.Net;
using Edge.Data.Pipeline;

namespace Edge.Services.FTP
{
	class FtpFileHandllerService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			//Deleting files from FTP server

			foreach (DeliveryFile file in this.Delivery.Files)
			{
				FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(file.SourceUrl);
				request.UseBinary = true;

				request.Credentials = new NetworkCredential(
					this.Delivery.Parameters["UserID"].ToString(),
					this.Delivery.Parameters["Password"].ToString()
					);

				request.Method = WebRequestMethods.Ftp.DeleteFile;
				FtpWebResponse response = (FtpWebResponse)request.GetResponse();
				response.Close();
				
			}

			return Core.Services.ServiceOutcome.Success;
		}
	}
}
