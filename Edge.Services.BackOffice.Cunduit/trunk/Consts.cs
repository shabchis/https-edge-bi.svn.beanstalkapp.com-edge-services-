using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Edge.Services.BackOffice.Cunduit
{
	public static class BoConfigurationOptions
	{
		public const string BO_XPath_Trackers = "Bo.Xpath";
		public const string BaseServiceAddress = "Bo.BaseServiceAdress";
		public const string UserName = "Bo.User";
		public const string Password = "Bo.Password";
		public const string BoFileName="BO.xml";







		public const string UtcOffset = "Bo.UTCOffest";
	}
	public static class BoFields
	{
		public const string ID = "gateway_id";
		public const string Signups = "Signups";
		public const string GoodToolbars = "GoodToolbars";
		public const string GreatToolbars = "GreatToolbars";
		public const string TotalInstalls = "TotalInstalls";
		
	}
	public class Consts
	{
		public const string XmlFileWithAttributes = "XmlFileWithAttributes";
	}
}
