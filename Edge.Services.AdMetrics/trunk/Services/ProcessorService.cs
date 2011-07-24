using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline.Services;
using Edge.Core.Data;
using Edge.Data.Objects;
using Edge.Data.Pipeline;

namespace Edge.Services.AdMetrics
{
	public class ProcessorService: PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{
			string fileName;
			if (!this.Instance.Configuration.Options.TryGetValue(Const.ConfigurationOptions.DeliveryFileName, out fileName))
				throw new ConfigurationException(String.Format("{0} is missing in the service configuration options.", Const.ConfigurationOptions.DeliveryFileName));

			DeliveryFile file = this.Delivery.Files[fileName];
			if (file == null)
				throw new Exception(String.Format("Could not find delivery file '{0}' in the delivery.", fileName));

			FileCompression compression;
			string compressionOption;
			if (this.Instance.Configuration.Options.TryGetValue(Const.ConfigurationOptions.Compression, out compressionOption))
			{
				if (!Enum.TryParse<FileCompression>(compressionOption, out compression))
					throw new ConfigurationException(String.Format("Invalid compression type '{0}'.", compressionOption));
			}
			else
				compression = FileCompression.None;

			using (var stream = file.OpenContents(compression: compression))
			{
				using (ReaderBase<dynamic> reader = this.CreateReader())
				{
					using (AdMetricsImportManager session = new AdMetricsImportManager(this.Instance.InstanceID))
					{
						session.OnAdUsidRequired = null; // TODO: create Ad USID

						while (reader.Read())
						{
							var ad = new Ad()
							{
								Campaign = new Campaign()
								{
									Account = new Account()
									{
										// TODO: sniff account ID
									},
									Channel = new Channel()
									{
										// TODO: channel
										
									},
									Name = null,
									OriginalID = null,
									Status = ObjectStatus.Unknown,
								},
								OriginalID = null,
								DestinationUrl = null,
								Name = null,
							};
						}
					}
				}
			}

			return Core.Services.ServiceOutcome.Success;
		}

		private ReaderBase<dynamic> CreateReader()
		{
			throw new NotImplementedException();
		}
	}
}
