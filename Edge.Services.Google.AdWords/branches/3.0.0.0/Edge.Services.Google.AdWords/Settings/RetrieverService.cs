using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Edge.Core.Configuration;
using Edge.Core.Services;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Services.Google.AdWords.Settings.Retrievers;
using Google.Api.Ads.AdWords.Lib;
using System.IO;

namespace Edge.Services.Google.AdWords.Settings
{
    public class RetrieverService : PipelineService
    {
		#region Override Methods
		protected override ServiceOutcome DoPipelineWork()
		{
			foreach (var clientId in (string[]) Delivery.Parameters["AdwordsClientIDs"])
			{
				//Get all files on specific client
				var files = Delivery.Files.Where(x => x.Parameters["AdwordsClientID"].ToString() == clientId);

				//Setting Adwords User and AuthToken
				var headers = new Dictionary<string, string>
					{
						{"DeveloperToken", Delivery.Parameters["DeveloperToken"].ToString()},
						{"UserAgent", FileManager.UserAgentString},
						{"EnableGzipCompression", "true"},
						{"ClientCustomerId", clientId},
						{"Email", Delivery.Parameters["MccEmail"].ToString()}
					};

				var adwrodsUser = new AdWordsUser(headers);
				var config = adwrodsUser.Config as AdWordsAppConfig;
				if (config == null)
					throw new Exception("Failed to convert AdwordUser.Config to AdWordsAppConfig");
				config.AuthToken = AdwordsUtill.GetAuthToken(adwrodsUser);

				var counter = 0;
				foreach (var file in files)
				{
					counter ++;

					// retrieve data using retriever of specific type
					var retriever = GetRetrieverByType(file.Parameters["ReportType"].ToString());
					var data = retriever.RetrieveData(file, adwrodsUser);

					// save retrieved data to file
					file.Location = file.CreateLocation();
					var fullPath = Path.Combine(AppSettings.Get(typeof(FileManager), "RootPath"), file.Location);
					if (!Directory.Exists(Path.GetDirectoryName(fullPath)))
						Directory.CreateDirectory(Path.GetDirectoryName(fullPath));

					File.WriteAllLines(fullPath, data);

					Progress = (double) counter/files.Count();
				}
			}

			Delivery.Save();
			return ServiceOutcome.Success;
		}

		#endregion

		#region Private Methods
		private IRetriever GetRetrieverByType(string type)
		{
			if (type == "CampaignCriterion")
				return new CampaignCriterionRetriever();

			throw new ConfigurationErrorsException(String.Format("Unsupported report type '{0}'", type));
		}
		#endregion
    }
}
