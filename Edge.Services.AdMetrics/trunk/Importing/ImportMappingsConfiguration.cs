using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Configuration;
using Edge.Services.AdMetrics.Configuration;
using Edge.Core.Configuration;
using System.Xml;

namespace Edge.Services.AdMetrics
{

	public enum IndexType
	{
		Simple,
		Segment,
		Measure
	}

	public class MapSpec
	{
		public MapSpec Parent { get; set; }
		public FieldInfo Property { get; set; }
		public object CollectionIndex { get; set; }
		public IndexType CollectionIndexType { get; set; }
		public Type ChildType { get; set; }
		public List<MapSpec> ChildTargets { get; private set; }
		public List<MapSource> Sources { get; private set; }

		public MapSpec()
		{
			ChildTargets = new List<MapSpec>();
			Sources = new List<MapSource>();
		}

		public Dictionary<Type, MapSpec[]> FromConfiguration(MappingConfiguration config)
		{
			var mappings = new Dictionary<Type, MapSpec[]>();
			throw new NotImplementedException();
			
		}
	}

	public class MapSource
	{
		public string Name;
		public string Field;
		public Regex Regex;
	}
}

namespace Edge.Services.AdMetrics.Configuration
{
	public class MappingConfiguration: ConfigurationElementCollectionBase<TypeElement>
	{
		protected override string ElementName { get { return "Type"; }}
		protected override ConfigurationElement  CreateNewElement() { return new TypeElement(); }
		public override ConfigurationElementCollectionType  CollectionType { get { return ConfigurationElementCollectionType.BasicMap; }}
		protected override object GetElementKey(ConfigurationElement element) { return (element as TypeElement).Name; }
	}

	public class TypeElement: ConfigurationElement
	{
		[ConfigurationProperty("Name")]
		public string Name
		{
			get { return base["Name"] as string; }
		}

		[ConfigurationProperty("", IsDefaultCollection = true)]
		public MapElementCollection ChildMappings
		{
			get { return (MapElementCollection)base[""]; }
		}
	}


	public class MapElementCollection : ConfigurationElementCollectionBase<ReadElement>
	{
		public override ConfigurationElementCollectionType CollectionType
		{
			get { return ConfigurationElementCollectionType.BasicMap; }
		}

		protected override ConfigurationElement CreateNewElement()
		{
			throw new NotImplementedException();
		}

		protected override bool OnDeserializeUnrecognizedElement(string elementName, System.Xml.XmlReader reader)
		{
			ReadElement elem;
			if (elementName == "Read")
				elem = new ReadElement();
			else if (elementName == "Map")
				elem = new MapElement();
			else
				return false;

			((ISerializableConfigurationElement)elem).Deserialize(reader);
			return true;
		}

		protected override object GetElementKey(ConfigurationElement element)
		{
			var read = (ReadElement) element;
			if (element is MapElement)
				return ((MapElement)element).To;
			else
				return read.Name ?? read.Field;
		}
	}

	public class ReadElement : ConfigurationElement, ISerializableConfigurationElement
	{
		[ConfigurationProperty("Name")]
		public string Name
		{
			get { return base["Name"] as string; }
		}

		[ConfigurationProperty("Field")]
		public string Field
		{
			get { return base["Field"] as string; }
		}

		[ConfigurationProperty("Regex")]
		public string Regex
		{
			get { return base["Regex"] as string; }
		}

		void ISerializableConfigurationElement.Deserialize(XmlReader reader)
		{
			this.DeserializeElement(reader, false);
		}

		void ISerializableConfigurationElement.Serialize(XmlWriter writer, string elementName)
		{
			this.SerializeToXmlElement(writer, elementName);
		}
	}

	public class MapElement : ReadElement
	{
		[ConfigurationProperty("To")]
		public string To
		{
			get { return base["To"] as string; }
		}

		[ConfigurationProperty("Value")]
		public string Value
		{
			get { return base["Value"] as string; }
		}

		[ConfigurationProperty("", IsDefaultCollection=true)]
		public MapElementCollection ChildMappings
		{
			get { return (MapElementCollection)base[""]; }
		}
	}

}
