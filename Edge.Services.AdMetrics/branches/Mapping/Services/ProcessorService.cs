using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Core.Data;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline.Mapping;

namespace Edge.Services.AdMetrics
{
	public class ProcessorService: PipelineService
	{
		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{ 
			// TODO: setup/defaults/configuration/etc.
			// ------------------------------------------
			
			/*
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
			*/

			MappingConfiguration mappingConfig = MappingConfiguration.Load("conduit-mappings.xml");

			mappingConfig.ExternalMethods.Add("GetChannelByName", new Func<string,string>(channelName => "whats up"));

			mappingConfig.Initialize();

			MappingContainer adMappings = mappingConfig.Objects[typeof(Ad)];
			MappingContainer metricsMappings = mappingConfig.Objects[typeof(AdMetricsUnit)];

			// ------------------------------------------
			
			/*
			using (var stream = file.OpenContents(compression: compression))
			{
				using (XmlDynamicReader reader = new XmlDynamicReader(stream, xpath))
				{
					Func<string, string> readFunction = requestedField => 
					{
						if (requestedField.StartsWith("@"))
							return reader.Current.Attributes[requestedField];
						else
							return reader.Current[requestedField];
					};

					using (AdMetricsImportManager session = new AdMetricsImportManager(this.Instance.InstanceID))
					{
						session.OnAdUsidRequired = null; // TODO: create Ad USID

						while (reader.Read())
						{
							var ad = new Ad();
							adMappings.Apply(ad, readFunction);
							session.ImportAd(ad);

							var metrics = new AdMetricsUnit();
							metricsMappings.Apply(metrics, readFunction);
							session.ImportMetrics(metrics);
						}

						session.EndImport();
					}
				}
			}
			*/

			return Core.Services.ServiceOutcome.Success;
		}

	}
}
