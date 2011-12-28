using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;
using Edge.Data.Objects;
using Edge.Data.Pipeline;
using System.Configuration;
using Edge.Core.Configuration;
using System.Xml;
using System.Collections;
using Edge.Core.Utilities;
using System.ComponentModel;
/*
namespace Edge.Data.Pipeline.ImportMapping
{
	/// <summary>
	/// 
	/// </summary>
	public class ReadSource
	{
		string _name;
		string _field;

		public string Name
		{
			get { return _name; }
			set
			{
				if (!Regex.IsMatch(value, "[A-Za-z_][A-Za-z0-9_]*"))
					throw new MappingException(String.Format("The read source name '{0}' is not valid because it includes illegal characters.", value));
				
				_name = value;
			}
		}

		public string Field
		{
			get { return _field; }
			set { _field = value; }
		}
		public Regex Regex;
	}


	/// <summary>
	/// 
	/// </summary>
	public class Assignment
	{
		public Assignment Parent { get; set; }
		public MemberInfo TargetMember { get; set; }
		public Type TargetMemberType { get; private set; }
		public object CollectionKey { get; set; }
		public Type NewObjectType { get; set; }
		public List<Assignment> SubAssignments { get; set; }
		public List<ReadSource> ReadSources { get; set; }
		public ValueExpression Value { get; set; }

		public Assignment(string propertyName)
		{
			SubAssignments = new List<Assignment>();
			ReadSources = new List<ReadSource>();
		}

		public ReadSource GetSource(string name, bool useParentSources = true)
		{
			throw new NotImplementedException();
		}

		public void Apply(object targetObject, Func<string, string> readFunction)
		{
			var readSources = new Dictionary<string, string>();
			this.Apply(targetObject, readFunction, readSources);
		}

		private void Apply(object targetObject, Func<string, string> readFunction, Dictionary<string, string> readSources)
		{
			bool newSources = false;

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
			foreach (ReadSource source in this.ReadSources)
			{
				if (!newSources)
				{
					// Duplicate for sources for this branch only
					newSources = true;
					readSources = new Dictionary<string, string>(readSources);
				}

				if (String.IsNullOrWhiteSpace(source.Field))
					throw new MappingException("The 'Field' property must be defined.");

				// Validate the name
				string name;
				bool usingFieldName = false;
				if ( String.IsNullOrWhiteSpace(source.Name))
				{
					name = source.Field;
					usingFieldName = true;
				}
				else
					name = source.Name;

				if (!Regex.IsMatch(name, "[A-Za-z_][A-Za-z0-9_]*"))
				{
					throw new MappingException(String.Format(usingFieldName ?
						"The field name '{0}' cannot be used as the read source name because it includes illegal characters. Please specify a separate 'Name' attribute.":
						"The read source name '{0}' is not valid because it includes illegal characters.",
						name
					));
				}

				string readValue = readFunction(source.Field);
				readSources[name] = readValue;

				// Capture groups
				if (source.Regex != null)
				{
					Match m = source.Regex.Match(readValue);
					foreach (string groupName in source.Regex.GetGroupNames())
					{
						readSources[name + "." + groupName] = m.Groups[groupName].Value;
					}

					
					//MatchCollection matches = source.Regex.Matches(source);
					//foreach (Match match in matches)
					//{
					//    if (!match.Success)
					//        continue;

					//    int fragmentCounter = 0;
					//    for (int g = 0; g < match.Groups.Count; g++)
					//    {
					//        Group group = match.Groups[g];
					//        string groupName = pattern.RawGroupNames[g];
					//        if (!group.Success || !AutoSegmentPattern.IsValidFragmentName(groupName))
					//            continue;

					//        // Save the fragment
					//        fragmentValues[pattern.Fragments[fragmentCounter++]] = group.Value;
					//    }
					//}
				}
			}

			// -------------------------------------------------------
			// STEP 3: FORMAT VALUE

			object mapValue;

			// Get the required value, if necessary
			if (Value != null)
			{
				mapValue = Value;
			}
			else if (NewObjectType != null)
			{
				// TODO-IF-EVER-THERE-IS-TIME-(YEAH-RIGHT): support constructor arguments

				try { mapValue = Activator.CreateInstance(this.NewObjectType); }
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
				object key = this.CollectionKeyLookup != null ?
					this.CollectionKeyLookup(this.CollectionKey) :
					this.CollectionKey;

				// Add the value to the collection
				if (currentCollection is IDictionary)
				{
					if (key == null)
						throw new MappingException(String.Format("Cannot use a null value as the key for the dictionary {0}.{1}.",
								targetObject.GetType().Name,
								this.TargetMember.Name,
								this.CollectionKey
							));

					var dict = (IDictionary)currentCollection;
					try { dict.Add(key, mapValue); }
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
					if (key != null)
					{
						if (key is Int32)
							list[(int)key] = mapValue;
						else
							throw new MappingException(String.Format("Cannot use the non-integer \"{2}\" as the index for the list {0}.{1}.",
								targetObject.GetType().Name,
								this.TargetMember.Name,
								this.CollectionKey
							));
					}
					else
						list.Add(mapValue);
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
				foreach (Assignment spec in this.SubAssignments)
				{
					// TODO: wrap this somehow for exception handling
					spec.Apply(value, readFunction, readSources);
				}
			}
		}
	}


	public class ValueExpression
	{
		public List<ValueExpressionComponent> Components;

		public T Ouput<T>()
		{
			return (T) this.Output(typeof(T));
		}

		public object Output(Type ouputType)
		{
			bool isstring = ouputType == typeof(string);
			TypeConverter converter = null;
			
			if (!isstring)
			{
				converter = TypeDescriptor.GetConverter(ouputType);
				if (converter == null)
					throw new MappingException(String.Format("Cannot convert string to {0}.", ouputType.FullName));
			}

			var output = new StringBuilder();	

			foreach (ValueExpressionComponent component in this.Components)
			{
				output.Append(component.Ouput());
			}

			string value = output.ToString();
			object returnValue;

			if (isstring)
			{
				// avoid compiler errors
				object o = output.ToString();
				returnValue = o;
			}
			else
			{
				if (!converter.IsValid(value))
					throw new MappingException(String.Format("'{0}' is not a valid value for {1}", value, ouputType.FullName));
				else
					returnValue = converter.ConvertFrom(value);
			}

			return returnValue;
		}
	}

	public abstract class ValueExpressionComponent
	{
		public abstract string Ouput();
	}

	public class FunctionInvokeComponent:ValueExpressionComponent
	{
		public string FunctionName;
		public List<ValueExpression> Parameters;
	}

	public class EvalComponent
	{
		public Evaluator Eval;
		public List<ValueExpression> Variables;
	}

	public class ReadSourceOuputComponent
	{
		public ReadSource ReadSource;
	}

	public class StringComponent
	{
		public string String;
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

	public class MappingConfiguration
	{
		public List<MappedObject> Objects = new List<MappedObject>();

		public static MappingConfiguration Load(XmlElement root)
		{
			var output = new MappingConfiguration();

			foreach (XmlElement objectXml in root.SelectNodes("Object"))
			{
				var objectMapping = new MappedObject();
				string typeName = objectXml.HasAttribute("Type") ? objectXml.GetAttribute("Type") : null;
				if (typeName == null)
					throw new MappingException("<Object>: 'Type' attribute is missing.");
				objectMapping.TargetType = Type.GetType(typeName, false);
				if (objectMapping.TargetType == null)
					throw new MappingException(String.Format("<Object>: Type '{0}' could not be found.", typeName));

				foreach(XmlNode node in objectXml.ChildNodes)
				{
					if (!(node is XmlElement))
						continue;

					var element = (XmlElement) node;
					if (element.Name == "Read")
					{
						var read = new ReadSource();
						if (!element.HasAttribute("Field"))
							throw new MappingException("<Read>: Missing 'Field' attribute.");
						read.Field = element.GetAttribute("Field");

						if (element.HasAttribute("Name"))
							read.Name = element.GetAttribute("Name");
					}
					else if (node.Name == "Map")
					{
					}
					else
					{
						throw new MappingException(String.Format("<Object>: Element {0} is invalid.", node.Name));
					}
				}
			}
		}
	}

	public class MappedObject
	{
		public Type TargetType;
		//public List<MapSpec>
	}
}
*/