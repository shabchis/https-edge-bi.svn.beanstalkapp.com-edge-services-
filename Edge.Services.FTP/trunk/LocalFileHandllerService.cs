using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Core.Configuration;

namespace Edge.Services.FTP
{
	class LocalFileHandllerService : PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			//CopyFiles to Watcher Directory
			if (Delivery.Files != null && Delivery.Files.Count > 0)
			{
				foreach (DeliveryFile file in this.Delivery.Files)
				{
					string source = System.IO.Path.Combine(AppSettings.Get(typeof(FileManager), "RootPath"), file.Location);
					string target = System.IO.Path.Combine(this.Delivery.Parameters["DirectoryWatcherLocation"].ToString(), System.IO.Path.GetFileName(source));
					System.IO.File.Copy(source, target, overwrite: true);
				}
			}

			return Core.Services.ServiceOutcome.Success;
		}
	}
}
