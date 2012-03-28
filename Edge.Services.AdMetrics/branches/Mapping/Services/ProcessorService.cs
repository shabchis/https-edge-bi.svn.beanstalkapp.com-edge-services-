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
		public Dictionary<string, Account> Accounts;
		public Dictionary<string, Channel> Channels;
		public AdMetricsImportManager ImportManager;

		protected override Core.Services.ServiceOutcome DoPipelineWork()
		{ 
			// TODO: setup/defaults/configuration/etc.
			// ------------------------------------------
			
			
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
			

			var mappingConfig = new MappingConfiguration();

			mappingConfig.ExternalMethods.Add("GetChannel", new Func<string, Channel>(channelName => new Channel() { Name = channelName }));
			mappingConfig.ExternalMethods.Add("GetAccount", new Func<string, Channel>(channelName => new Channel() { Name = channelName }));
			
			mappingConfig.OnFieldRead = field => "chinese";
			mappingConfig.Load("conduit-mappings.xml");

			MappingContainer adMappings = mappingConfig.Objects[typeof(Ad)];
			MappingContainer metricsMappings = mappingConfig.Objects[typeof(AdMetricsUnit)];

			Ad ad = new Ad();
			adMappings.Apply(ad);

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

		public Account GetAccount(string name)
		{
			Account a;
			if (!Accounts.TryGetValue(name, out a))
				throw new MappingException(String.Format("No account named '{0}' could be found, or it cannot be used from within account #{1}.", name, Instance.AccountID));
			return a;
		}

		public Channel GetChannel(string name)
		{
			Channel c;
			if (!Channels.TryGetValue(name, out c))
				throw new MappingException(String.Format("No channel named '{0}' could be found.", name));
			return c;
		}

		public Segment GetSegment(string name)
		{
			Segment s;
			if (!ImportManager.Segments.TryGetValue(name, out s))
				throw new MappingException(String.Format("No segment named '{0}' could be found.", name));
			return s;
		}

		public Measure GetMeasure(string name)
		{
			Measure m;
			if (!ImportManager.Measures.TryGetValue(name, out m))
				throw new MappingException(String.Format("No measure named '{0}' could be found. Make sure you specified the base measure name, not the display name.", name));
			return m;
		}

		public SegmentValue GetAutoSegmentValue(string segment, string source)
		{
			return this.AutoSegments.ExtractSegmentValue(GetSegment(segment), source);
		}

		public DateTime CreatePeriod(DateTimeSpecificationAlignment align, string year, string month, string day)//, string hour = null, string minute = null, string second = null )
		{
			DateTime baseDateTime;
			try { baseDateTime = new DateTime(Int32.Parse(year), Int32.Parse(month), Int32.Parse(day)); }
			catch (Exception ex)
			{
				throw new MappingException(String.Format("Could not parse the date parts (y = '{0}', m = '{1}', d = '{2}'.", year, month, day), ex);
			}

			DateTime period;
			period = new DateTimeSpecification()
				{
					Alignment = align,
					BaseDateTime = baseDateTime
				}
				.ToDateTime();

			return period;
		}
	}
}
