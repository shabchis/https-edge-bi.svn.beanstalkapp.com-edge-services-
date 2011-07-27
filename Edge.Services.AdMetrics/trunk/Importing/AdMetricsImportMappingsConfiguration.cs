using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;

namespace Edge.Services.AdMetrics.Configuration
{
	public class ImportMappingsConfiguration
	{
	}

	public class ImportMapping
	{
		public List<MappingSource> MappingSources;
		public MappingTarget MappingTarget;
	}

	public class MappingSource
	{
		public string Name;
		public string Field;
		public Regex Regex;
	}

	public class MappingTarget
	{
		public MappingTargetPath Property;
		public Type NewObjectType;
	}

	public class MappingTargetPath
	{
		public Type ObjectType;
		public FieldInfo FieldInfo;
	}

	public class MappingPath
	{
	}
}
