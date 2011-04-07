using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using MyFacebook=Facebook;
using System.Net;

namespace Edge.Services.Facebook.AdsApi
{
	class RetriverService : PipelineService
	{
		protected override ServiceOutcome DoPipelineWork()
		{
			double progress = 0;
			foreach (DeliveryFile file in this.Delivery.Files)
			{
				HttpWebResponse response = null;
				try
				{
					HttpWebRequest request = (HttpWebRequest)file.Parameters["HttpRequest"];
					response = (HttpWebResponse)request.GetResponse();
					FileManager.Download(response.GetResponseStream(), null);
					progress += 0.249;
					this.ReportProgress(progress);
				}
				catch (WebException ex)
				{
					Console.WriteLine(ex.Message);
				}
			}

			return ServiceOutcome.Success;
		}
	}
}
