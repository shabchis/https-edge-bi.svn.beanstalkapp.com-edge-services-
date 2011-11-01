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
using System.Collections;

namespace Edge.Services.AdMetrics
{

	public enum IndexType
	{
		Auto,
		Segment,
		Measure
	}

	public class MapSpec
	{
		public MapSpec Parent { get; set; }
		public MemberInfo TargetMember { get; set; }
		public Type TargetMemberType { get; private set; }
		public object CollectionKey { get; set; }
		public IndexType CollectionKeyType { get; set; }
		public Type NewObjectType { get; set; }
		public List<MapSpec> SubSpecs { get; set; }
		public List<MapSource> Sources { get; set; }
		public object Value { get; set; }

		public MapSpec(string propertyName)
		{
			SubSpecs = new List<MapSpec>();
			Sources = new List<MapSource>();
		}

		public static Dictionary<Type, MapSpec[]> LoadFromConfiguration(MappingConfiguration config)
		{
			var mappings = new Dictionary<Type, MapSpec[]>();
			throw new NotImplementedException();
		}

		public MapSource GetSource(string name, bool useParentSources = true)
		{
			throw new NotImplementedException();
		}


		public void Apply(object targetObject, Func<string,string> readFunction, Dictionary<string,string> sources)
		{
			// -------------------------------------------------------
			// STEP 1: COLLECTIONS

			// Determine if we're dealing with a collection
			object currentCollection = null;
			bool newCollection = false;
			if (this.TargetMemberType.IsAssignableFrom(typeof(ICollection)))
			{
				// Check if we need to create a new collection
				if (this.TargetMember is PropertyInfo)
					currentCollection = (this.TargetMember as PropertyInfo).GetValue(targetObject, null);
				else if (this.TargetMember is FieldInfo)
					currentCollection = (this.TargetMember as FieldInfo).GetValue(targetObject);

				// No collection found, create one
				if (currentCollection == null)
				{
					newCollection = true;
					try { currentCollection = Activator.CreateInstance(this.TargetMemberType); }
					catch (Exception ex)
					{
						throw new MappingException(string.Format(
							"Could not initialize the collection for {0}.{1}. See inner exception for more details.",
								targetObject.GetType().Name,
								this.TargetMember.Name
							), ex);
					}
				}
			}

			// -------------------------------------------------------
			// STEP 2: READ FROM SOURCE
			//foreach(

			// -------------------------------------------------------
			// STEP 3: FORMAT VALUE

			// Get the required value, if necessary
			object value = null;
			if (Value != null)
			{
				value = Value;
			}
			else if (NewObjectType != null)
			{
				// TODO-IF-EVER-THERE-IS-TIME-(YEAH-RIGHT): support constructor arguments

				try { value = Activator.CreateInstance(this.NewObjectType); }
				catch(Exception ex)
				{
					throw new MappingException(string.Format(
						"Could not create new instance of {0} for applying to {1}.{2}. See inner exception for more details.",
						this.NewObjectType.FullName,
						targetObject.GetType().Name,
						this.TargetMember.Name
					), ex);
				}
			}

			// -------------------------------------------------------
			// STEP 4: APPLY VALUE

			// Apply the value
			if (currentCollection != null)
			{
				// Add the value to the collection
				if (currentCollection is IDictionary)
				{
					var dict = (IDictionary)currentCollection;
					try { dict.Add(this.CollectionKey, value); }
					catch (Exception ex)
					{
						throw new MappingException(String.Format("Could not add the value to the dictionary {0}.{1}. See inner exception for more details.",
							targetObject.GetType().Name,
							this.TargetMember.Name
						), ex);
					}
				}
				else if (currentCollection is IList)
				{
					var list = (IList)currentCollection;
					if (this.CollectionKey != null)
					{
						if (this.CollectionKey is Int32)
							list[(int)this.CollectionKey] = value;
						else
							throw new MappingException(String.Format("Cannot use the non-integer \"{2}\" as the index for the list {0}.{1}.",
								targetObject.GetType().Name,
								this.TargetMember.Name,
								this.CollectionKey
							));
					}
					else
						list.Add(value);
				}
				else
				{
					throw new MappingException(String.Format("The collection {0}.{1} cannot be used as a mapping target because it does not implement either IList or IDictionary.",
						targetObject.GetType().Name,
						this.TargetMember.Name
					));
				}

				// Apply the collection, if it is new
				if (newCollection)
				{
					try
					{
						if (this.TargetMember is PropertyInfo)
							(this.TargetMember as PropertyInfo).SetValue(targetObject, currentCollection, null);
						else if (this.TargetMember is FieldInfo)
							(this.TargetMember as FieldInfo).SetValue(targetObject, currentCollection);
					}
					catch (Exception ex)
					{
						throw new MappingException(string.Format(
							"Could not apply a collection to {0}.{1}. See inner exception for more details.",
								targetObject.GetType().Name,
								this.TargetMember.Name
							), ex);
					}
				}

			}
			else
			{
				// Apply the value directly to the member
				try
				{
					if (this.TargetMember is PropertyInfo)
						(this.TargetMember as PropertyInfo).SetValue(targetObject, value, null);
					else if (this.TargetMember is FieldInfo)
						(this.TargetMember as FieldInfo).SetValue(targetObject, value);
				}
				catch (Exception ex)
				{
					throw new MappingException(String.Format("Could not apply the value to the member {0}.{1}. See inner exception for more details.",
						targetObject.GetType().Name,
						this.TargetMember.Name
					), ex);
				}
			}

			// -------------------------------------------------------
			// STEP 5: RECURSION

			// Activate child mappings on the value
			if (value != null)
			{
				foreach (MapSpec spec in this.SubSpecs)
				{
					// TODO: wrap this somehow for exception handling
					spec.Apply(value, readFunction);
				}
			}
		}
	}

	public class MapSource
	{
		public string Name;
		public string Field;
		public Regex Regex;
		public string Value;
	}

	public enum MapSourceType
	{
		Field,
		Regex,
		Method
	}

	[Serializable]
	public class MappingException : Exception
	{
		public MappingException() { }
		public MappingException(string message) : base(message) { }
		public MappingException(string message, Exception inner) : base(message, inner) { }
		protected MappingException(
		  System.Runtime.Serialization.SerializationInfo info,
		  System.Runtime.Serialization.StreamingContext context)
			: base(info, context) { }
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
