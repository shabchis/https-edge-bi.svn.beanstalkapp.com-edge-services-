using System;
using System.Collections.Generic;
using Edge.Data.Objects;

namespace Edge.Services.AdMetrics
{


	class example
	{
		static void Main()
		{
			MapSpec test;

			// ======================================================
			// <Map To="Campaign.Channel.Name" Field="Channel"/>
			test = new MapSpec()
			{
				TargetMember = typeof(Ad).GetProperty("Campaign"),
				NewObjectType = typeof(Ad).GetProperty("Campaign").PropertyType,
				SubSpecs = new List<MapSpec>()
				{
					new MapSpec()
					{
						TargetMember = typeof(Ad).GetProperty("Campaign").PropertyType.GetProperty("Channel"),
						SubSpecs = new List<MapSpec>()
						{
							new MapSpec()
							{
								TargetMember = typeof(Ad).GetProperty("Campaign").PropertyType.GetProperty("Channel").PropertyType.GetProperty("Name"),
								Sources = new List<MapSource>()
								{
									new MapSource() { Name = "Channel", Field = "Channel" }
								}
							}
						},
						Value = "{Channel}"
					}
				}
			};


			// ======================================================
			//<Map To="Creatives[]::TextCreative">
			//    <Read Field="Desc1"/>
			//    <Read Field="Desc2"/>
			//    <Map To="TextType" Value="Body"/>
			//    <Map To="Text" Value="{Desc1}"/>
			//    <Map To="Text2" Value="{Desc2}"/>
			//</Map>
			test = new MapSpec("Creatives")
			{
				NewObjectType = typeof(TextCreative),
				Sources = new List<MapSource>()
				{
					new MapSource() { Field = "Desc1" },
					new MapSource() { Field = "Desc2" }
				},
				SubSpecs = new List<MapSpec>()
				{
					new MapSpec("TextType") { Value = TextCreativeType.Body }
				}
			};

		}
	}

}