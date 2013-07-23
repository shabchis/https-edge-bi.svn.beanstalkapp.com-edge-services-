using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml.Serialization;

namespace Edge.Services.Google.AdWords
{
	[XmlRoot(ElementName = "GoogleAdwordsReportConfig")]
	public class AdwordsReportConfig
	{
		[XmlElement("Report")]
		public List<AdwordsReport> Reports { get; set; }

		#region Serialization
		public static string Serialize(AdwordsReportConfig config)
		{
			var ser = new XmlSerializer(typeof(AdwordsReportConfig));
			using (var textWriter = new StringWriter())
			{
				ser.Serialize(textWriter, config);
				return textWriter.ToString();
			}
		}

		public static AdwordsReportConfig Deserialize(string xml)
		{
			var ser = new XmlSerializer(typeof(AdwordsReportConfig));
			using (var reader = new StringReader(xml))
			{
				return ser.Deserialize(reader) as AdwordsReportConfig;
			}
		} 
		#endregion
	}

	public class AdwordsReport
	{
		[XmlAttribute(AttributeName = "Name")]
		public string Name { get; set; }

		[XmlAttribute(AttributeName = "Type")]
		public string Type { get; set; }

		[XmlAttribute(AttributeName = "Filter")]
		public string Filter { get; set; }

		[XmlAttribute(AttributeName = "Enable")]
		public bool Enable { get; set; }

		[XmlElement("Field")]
		public List<AdwordsReportField> Fields { get; set; }

		public string GetFieldList()
		{
			var fieldStr = new StringBuilder();
			foreach (var field in Fields)
			{
				fieldStr.AppendFormat("{0},", field.Name);
			}
			return fieldStr.Length > 0 ? fieldStr.Remove(fieldStr.Length - 1, 1).ToString() : string.Empty;
		}
	}

	public class AdwordsReportField
	{
		[XmlAttribute(AttributeName = "Name")]
		public string Name { get; set; }
	}
}
