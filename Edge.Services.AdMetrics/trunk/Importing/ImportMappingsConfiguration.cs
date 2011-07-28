using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;
using Edge.Data.Objects;

namespace Edge.Services.AdMetrics.Configuration
{
	public class ImportMappingsConfiguration
	{
	}

	public class ImportMapping
	{
		public List<MappingSource> MappingSources;
		public List<MappingTarget> MappingTargets;
	}

	public class MappingSource
	{
		public string Name;
		public string Field;
		public Regex Regex;
	}

	public class MappingTarget
	{
		public Type ObjectType;
		public FieldInfo[] Path;
	}

	public class MappingPathSegment
	{
		public FieldInfo FieldInfo;
		public object Indexer;
	}


	class test
	{
		static void Main()
		{
			var mappings = new List<ImportMapping>()
			{
				// <Map To="Ad.Campaign.Channel.Name" Field="Channel"/>
				new ImportMapping()
				{
					MappingSources = new List<MappingSource>()
					{
						new MappingSource() { Field="Channel" }
					},
					MappingTargets = new List<MappingTarget>()
					{
						new MappingTarget()
						{
							ObjectType = typeof(Ad),
							Path = new FieldInfo[]
							{
								typeof(Ad).GetField("Campaign"),
								typeof(Ad).GetField("Channel"),
								typeof(Ad).GetField("Name"),
							}
						}
					}
				},


			};
		}
	}

}
