using System;
using System.Collections.Generic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using GA = Google.Api.Ads.AdWords.v201302;
using System.IO;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Metrics;
using Edge.Data.Pipeline.Common.Importing;
using System.Linq;


namespace Edge.Services.Google.AdWords
{
    class ProcessorService : MetricsProcessorServiceBase
    {
        static ExtraField NetworkType = new ExtraField() { ColumnIndex = 1, Name = "NetworkType" };
        static ExtraField AdType = new ExtraField() { ColumnIndex = 2, Name = "adType" };
        static Dictionary<string, EdgeAdType> GoogleAdTypeDic;
        static Dictionary<string, string> GoogleMeasuresDic;
        static Dictionary<string, ObjectStatus> ObjectStatusDic;

        static enum EdgeAdType
        {
            Text_ad = 1,
            Flash = 2,
            Image_ad = 3,
            Display_ad = 4,
            Product_listing_ad = 5,
            Mobile_ad = 6,
            Local_business_ad = 7,
            Third_party_ad = 8,
            Other = 9,
            Mobile_text = 10,
            Mobile_image = 11,
            Mobile_display = 12

        }

        public ProcessorService()
        {
            GoogleAdTypeDic = new Dictionary<string, EdgeAdType>()
			{
				{Const.AdTypeValues.Text_ad,EdgeAdType.Text_ad},
				{Const.AdTypeValues.Flash_ad,EdgeAdType.Flash},
				{Const.AdTypeValues.Image_ad,EdgeAdType.Image_ad},
				{Const.AdTypeValues.Display_ad,EdgeAdType.Display_ad},
				{Const.AdTypeValues.Product_listing_ad,EdgeAdType.Product_listing_ad},
				{Const.AdTypeValues.Mobile_ad,EdgeAdType.Mobile_ad},
				{Const.AdTypeValues.Local_business_ad,EdgeAdType.Local_business_ad},
				{Const.AdTypeValues.Third_party_ad,EdgeAdType.Third_party_ad},
				{Const.AdTypeValues.Other,EdgeAdType.Other},
                {Const.AdTypeValues.Mobile_text,EdgeAdType.Mobile_text},
                {Const.AdTypeValues.Mobile_image,EdgeAdType.Mobile_image},
                {Const.AdTypeValues.Mobile_display,EdgeAdType.Mobile_display}


			};
            GoogleMeasuresDic = new Dictionary<string, string>()
			{
				{"Lead","Leads"},
				{"Signup","Signups"},
				{"Purchase","Purchases"},
				{"Purchase/Sale","Purchases"},
				{"Pageview","PageViews"},
				{"Default","Default"},
				{Const.ConversionOnePerClickFieldName,"TotalConversionsOnePerClick"},
				{Const.ConversionManyPerClickFieldName,"TotalConversionsManyPerClick"}
			};

            ObjectStatusDic = new Dictionary<string, ObjectStatus>()
			{
				{"PAUSED",ObjectStatus.Paused},
				{"DISABLED",ObjectStatus.Paused},
				{"DELETED",ObjectStatus.Deleted},
				{"ACTIVE",ObjectStatus.Active},
				{"ENABLED",ObjectStatus.Active}
			};
        }

        public new AdMetricsImportManager ImportManager
        {
            get { return (AdMetricsImportManager)base.ImportManager; }
            set { base.ImportManager = value; }
        }

        protected override Core.Services.ServiceOutcome DoPipelineWork()
        {
            bool includeConversionTypes = Boolean.Parse(this.Delivery.Parameters["includeConversionTypes"].ToString());
            bool includeDisplaytData = Boolean.Parse(this.Delivery.Parameters["includeDisplaytData"].ToString());

            //Status Members
            Dictionary<string, ObjectStatus> kwd_Status_Data = new Dictionary<string, ObjectStatus>();
            Dictionary<string, ObjectStatus> placement_kwd_Status_Data = new Dictionary<string, ObjectStatus>();
            Dictionary<Int64, ObjectStatus> adGroup_Status_Data = new Dictionary<Int64, ObjectStatus>();
            Dictionary<Int64, ObjectStatus> ad_Status_Data = new Dictionary<Int64, ObjectStatus>();
            Dictionary<Int64, ObjectStatus> campaign_Status_Data = new Dictionary<Int64, ObjectStatus>();

            using (this.ImportManager = new AdMetricsImportManager(this.Instance.InstanceID, new MetricsImportManagerOptions()
            {

                MeasureOptions = MeasureOptions.IsTarget | MeasureOptions.IsCalculated | MeasureOptions.IsBackOffice,
                MeasureOptionsOperator = OptionsOperator.Not,
                SegmentOptions = Data.Objects.SegmentOptions.All,
                SegmentOptionsOperator = OptionsOperator.And
            }))
            {

                #region Getting Keywords Status Data
                DeliveryFile _keyWordsStatusFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT] + "_Status"];
                string[] requiredHeaders = new string[1];
                requiredHeaders[0] = Const.AdPreRequiredHeader;
                var _keywordsStatusReader = new CsvDynamicReader(_keyWordsStatusFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                _keywordsStatusReader.MatchExactColumns = false;

                using (_keywordsStatusReader)
                {
                    while (_keywordsStatusReader.Read())
                    {
                        if (_keywordsStatusReader.Current[Const.KeywordIdFieldName] == Const.EOF)
                            break;

                        string kwdStatus = Convert.ToString(_keywordsStatusReader.Current[Const.KeywordStatusFieldName]);
                        Int64 kwdId = Convert.ToInt64(_keywordsStatusReader.Current[Const.KeywordIdFieldName]);
                        KeywordPrimaryKey keywordPrimaryKey = new KeywordPrimaryKey()
                        {
                            KeywordId = Convert.ToInt64(_keywordsStatusReader.Current[Const.KeywordIdFieldName]),
                            AdgroupId = Convert.ToInt64(_keywordsStatusReader.Current[Const.AdGroupIdFieldName]),
                            CampaignId = Convert.ToInt64(_keywordsStatusReader.Current[Const.CampaignIdFieldName])

                        };
                        kwd_Status_Data.Add(keywordPrimaryKey.ToString(), ObjectStatusDic[kwdStatus.ToUpper()]);
                    }
                }
                #endregion


                #region Getting Keywords Data
                Dictionary<string, double> _totals = new Dictionary<string, double>();
                DeliveryFile _keyWordsFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT]];
                requiredHeaders[0] = Const.AdPreRequiredHeader;
                var _keywordsReader = new CsvDynamicReader(_keyWordsFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                _keywordsReader.MatchExactColumns = false;
                Dictionary<string, KeywordTarget> _keywordsData = new Dictionary<string, KeywordTarget>();

                using (_keywordsReader)
                {
                    while (_keywordsReader.Read())
                    {
                        if (_keywordsReader.Current[Const.KeywordIdFieldName] == Const.EOF)
                            break;
                        KeywordPrimaryKey keywordPrimaryKey = new KeywordPrimaryKey()
                        {
                            KeywordId = Convert.ToInt64(_keywordsReader.Current[Const.KeywordIdFieldName]),
                            AdgroupId = Convert.ToInt64(_keywordsReader.Current[Const.AdGroupIdFieldName]),
                            CampaignId = Convert.ToInt64(_keywordsReader.Current[Const.CampaignIdFieldName])

                        };
                        KeywordTarget keyword = new KeywordTarget()
                        {
                            OriginalID = _keywordsReader.Current[Const.KeywordIdFieldName],
                            Keyword = _keywordsReader.Current[Const.KeywordFieldName],
                            Status = kwd_Status_Data[keywordPrimaryKey.ToString()]

                        };

                        keyword.QualityScore = Convert.ToString(_keywordsReader.Current[Const.QualityScoreFieldName]);
                        string matchType = _keywordsReader.Current[Const.MatchTypeFieldName];

                        //Setting Tracker for Keyword
                        if (!String.IsNullOrWhiteSpace(Convert.ToString(_keywordsReader.Current[Const.DestUrlFieldName])))
                        {
                            keyword.DestinationUrl = Convert.ToString(_keywordsReader.Current[Const.DestUrlFieldName]);
                        }
                        _keywordsData.Add(keywordPrimaryKey.ToString(), keyword);
                    }
                }
                #endregion

                Dictionary<string, PlacementTarget> _placementsData = new Dictionary<string, PlacementTarget>();

                #region Getting Placements Status Data
                DeliveryFile _placementsStatusFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT] + "_Status"];
                requiredHeaders[0] = Const.AdPreRequiredHeader;
                var _placementsStatusReader = new CsvDynamicReader(_placementsStatusFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                _placementsStatusReader.MatchExactColumns = false;

                using (_placementsStatusReader)
                {
                    while (_placementsStatusReader.Read())
                    {
                        if (_placementsStatusReader.Current[Const.KeywordIdFieldName] == Const.EOF)
                            break;

                        string placementsStatus = Convert.ToString(_placementsStatusReader.Current[Const.PlacementStatusFieldName]);
                        KeywordPrimaryKey placementPrimaryKey = new KeywordPrimaryKey()
                        {
                            KeywordId = Convert.ToInt64(_placementsStatusReader.Current[Const.KeywordIdFieldName]),
                            AdgroupId = Convert.ToInt64(_placementsStatusReader.Current[Const.AdGroupIdFieldName]),
                            CampaignId = Convert.ToInt64(_placementsStatusReader.Current[Const.CampaignIdFieldName])
                        };

                        placement_kwd_Status_Data.Add(placementPrimaryKey.ToString(), ObjectStatusDic[placementsStatus.ToUpper()]);
                    }
                }
                #endregion

                #region Getting Placements Data


                DeliveryFile _PlacementsFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT]];
                var _PlacementsReader = new CsvDynamicReader(_PlacementsFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                using (_PlacementsReader)
                {
                    while (_PlacementsReader.Read())
                    {
                        if (_PlacementsReader.Current[Const.KeywordIdFieldName] == Const.EOF)
                            break;
                        KeywordPrimaryKey placementPrimaryKey = new KeywordPrimaryKey()
                        {
                            KeywordId = Convert.ToInt64(_PlacementsReader.Current[Const.KeywordIdFieldName]),
                            AdgroupId = Convert.ToInt64(_PlacementsReader.Current[Const.AdGroupIdFieldName]),
                            CampaignId = Convert.ToInt64(_PlacementsReader.Current[Const.CampaignIdFieldName])
                        };
                        PlacementTarget placement = new PlacementTarget()
                        {
                            OriginalID = _PlacementsReader.Current[Const.KeywordIdFieldName],
                            Placement = _PlacementsReader.Current[Const.PlacementFieldName],
                            PlacementType = PlacementType.Managed,
                            Status = placement_kwd_Status_Data[placementPrimaryKey.ToString()]
                        };
                        //Setting Tracker for placment
                        if (!String.IsNullOrWhiteSpace(Convert.ToString(_PlacementsReader.Current[Const.DestUrlFieldName])))
                            placement.DestinationUrl = Convert.ToString(_PlacementsReader.Current[Const.DestUrlFieldName]);

                        _placementsData.Add(placementPrimaryKey.ToString(), placement);
                    }
                }
                #endregion


                #region Getting Conversions Data
                //Get Ads Conversion ( for ex. signup , purchase )

                DeliveryFile _conversionFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Conv"];
                var _conversionsReader = new CsvDynamicReader(_conversionFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                Dictionary<string, Dictionary<string, long>> importedAdsWithConv = new Dictionary<string, Dictionary<string, long>>();

                using (_conversionsReader)
                {
                    while (_conversionsReader.Read())
                    {
                        if (_conversionsReader.Current[Const.AdIDFieldName] == Const.EOF) // if end of report
                            break;
                        string conversionKey = String.Format("{0}#{1}", _conversionsReader.Current[Const.AdIDFieldName], _conversionsReader.Current[Const.KeywordIdFieldName]);
                        Dictionary<string, long> conversionDic = new Dictionary<string, long>();

                        if (!importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
                        {
                            //ADD conversionKey to importedAdsWithConv
                            //than add conversion field to importedAdsWithConv : <conversion name , conversion value>
                            Dictionary<string, long> conversion = new Dictionary<string, long>();
                            conversion.Add(Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurposeFieldName]), Convert.ToInt64(_conversionsReader.Current[Const.ConversionManyPerClickFieldName]));
                            importedAdsWithConv.Add(conversionKey, conversion);
                        }
                        else // if Key exists
                        {
                            // if current add already has current conversion type than add value to the current type
                            if (!conversionDic.ContainsKey(Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurposeFieldName])))
                                conversionDic.Add(Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurposeFieldName]), Convert.ToInt64(_conversionsReader.Current[Const.ConversionManyPerClickFieldName]));
                            // else create new conversion type and add the value
                            else
                                conversionDic[Convert.ToString(_conversionsReader.Current[Const.ConversionTrackingPurposeFieldName])] += Convert.ToInt64(_conversionsReader.Current[Const.ConversionManyPerClickFieldName]);
                        }
                    }
                }
                #endregion


                #region Getting Creative Status Data Ad,Adgroups,Campaign
                DeliveryFile _creativeStatusFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT] + "_Status"];
                requiredHeaders[0] = Const.AdIDFieldName;
                var _creativeStatusReader = new CsvDynamicReader(_creativeStatusFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                _creativeStatusReader.MatchExactColumns = false;

                using (_creativeStatusReader)
                {
                    while (_creativeStatusReader.Read())
                    {
                        if (_creativeStatusReader.Current[Const.AdIDFieldName] == Const.EOF)
                            break;

                        //Get Ad Status
                        string adStatus = Convert.ToString(_creativeStatusReader.Current[Const.AdStatusFieldName]);
                        Int64 adId = Convert.ToInt64(_creativeStatusReader.Current[Const.AdIDFieldName]);
                        ad_Status_Data.Add(adId, ObjectStatusDic[adStatus.ToUpper()]);

                        //Get Adgroup Status
                        string adGroupStatus = Convert.ToString(_creativeStatusReader.Current[Const.AdGroupStatusFieldName]);
                        Int64 adGroupId = Convert.ToInt64(_creativeStatusReader.Current[Const.AdGroupIdFieldName]);
                        if (!adGroup_Status_Data.ContainsKey(adGroupId))
                            adGroup_Status_Data.Add(adGroupId, ObjectStatusDic[adGroupStatus.ToUpper()]);

                        //Get Campaign Status
                        string campaignStatus = Convert.ToString(_creativeStatusReader.Current[Const.CampaignStatusFieldName]);
                        Int64 campaignId = Convert.ToInt64(_creativeStatusReader.Current[Const.CampaignIdFieldName]);
                        if (!campaign_Status_Data.ContainsKey(campaignId))
                            campaign_Status_Data.Add(campaignId, ObjectStatusDic[campaignStatus.ToUpper()]);
                    }
                }
                #endregion

                #region Getting Ads Data

                DeliveryFile _adPerformanceFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT]];
                var _adsReader = new CsvDynamicReader(_adPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                Dictionary<string, Ad> importedAds = new Dictionary<string, Ad>();

                //session.Begin(false);
                this.ImportManager.BeginImport(this.Delivery);

                DeliveryOutput currentOutput = Delivery.Outputs.First();

                foreach (KeyValuePair<string, Measure> measure in this.ImportManager.Measures)
                {
                    if (measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
                    {
                        _totals.Add(measure.Key, 0);
                    }

                }

                using (_adsReader)
                {
                    this.Mappings.OnFieldRequired = field => _adsReader.Current[field];

                    while (_adsReader.Read())
                    {


                        // Adding totals line for validation (checksum)
                        if (_adsReader.Current[Const.AdIDFieldName] == Const.EOF)
                        {
                            foreach (KeyValuePair<string, Measure> measure in this.ImportManager.Measures)
                            {
                                if (!measure.Value.Options.HasFlag(MeasureOptions.ValidationRequired))
                                    continue;

                                switch (measure.Key)
                                {
                                    case Measure.Common.Clicks: _totals[Measure.Common.Clicks] = Convert.ToInt64(_adsReader.Current.Clicks); break;
                                    case Measure.Common.Cost: _totals[Measure.Common.Cost] = (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000; break;
                                    case Measure.Common.Impressions: _totals[Measure.Common.Impressions] = Convert.ToInt64(_adsReader.Current.Impressions); break;
                                }
                            }
                            break;
                        }

                        AdMetricsUnit adMetricsUnit = new AdMetricsUnit();
                        adMetricsUnit.Output = currentOutput;
                        Ad ad;

                        string adId = _adsReader.Current[Const.AdIDFieldName];
                        if (!importedAds.ContainsKey(adId))
                        {
                            ad = new Ad();
                            ad.OriginalID = adId;
                            ad.Channel = new Channel() { ID = 1 };
                            ad.Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_adPerformanceFile.Parameters["AdwordsClientID"] };
                            ad.Status = ad_Status_Data[Convert.ToInt64(adId)];

                            //Ad Type
                            string adType = Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]);
                            string devicePreference = Convert.ToString(_adsReader.Current[Const.AdDevicePreferenceFieldName]);
                           
                            //incase of mobile ad 
                            if (devicePreference.Equals(Const.AdDevicePreferenceMobileFieldValue))
                                adType = string.Format("Mobile {0}",Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]));

                            ad.ExtraFields[AdType] = GoogleAdTypeDic[adType];

                            //Creative
                            ad.Creatives.Add(new TextCreative { TextType = TextCreativeType.DisplayUrl, Text = _adsReader.Current[Const.DisplayURLFieldName] });

                            ////Setting Tracker for Ad
                            if (!String.IsNullOrWhiteSpace(_adsReader.Current[Const.DestUrlFieldName]))
                            {
                                ad.DestinationUrl = _adsReader.Current[Const.DestUrlFieldName];

                                if (this.Mappings != null && this.Mappings.Objects.ContainsKey(typeof(Ad)))
                                    this.Mappings.Objects[typeof(Ad)].Apply(ad);
                            }

                            ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]] = new Campaign()
                            {
                                OriginalID = _adsReader.Current[Const.CampaignIdFieldName],
                                Name = _adsReader.Current[Const.CampaignFieldName],
                                Status = campaign_Status_Data[Convert.ToInt64(_adsReader.Current[Const.CampaignIdFieldName])]

                            };

                            //Image Type > Create Image
                            if (String.Equals(Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]), "Image ad"))
                            {
                                string adNameField = _adsReader.Current[Const.AdFieldName];
                                string[] imageParams = adNameField.Trim().Split(new Char[] { ':', ';' }); // Ad name: 468_60_Test7options_Romanian.swf; 468 x 60
                                ad.Name = imageParams[1].Trim();
                                ad.Creatives.Add(new ImageCreative()
                                {
                                    ImageUrl = imageParams[1].Trim(),
                                    ImageSize = imageParams[2].Trim()
                                });
                            }

                            else //Text ad or Display ad
                            {
                                ad.Name = _adsReader.Current[Const.AdFieldName];
                                ad.Creatives.Add(new TextCreative
                                {
                                    TextType = TextCreativeType.Title,
                                    Text = _adsReader.Current.Ad,
                                });
                                ad.Creatives.Add(new TextCreative
                                {
                                    TextType = TextCreativeType.Body,
                                    Text = _adsReader.Current["Description line 1"],
                                    Text2 = _adsReader.Current["Description line 2"]
                                });
                            }

                            //Insert Adgroup 
                            ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.AdGroup]] = new AdGroup()
                            {
                                Campaign = (Campaign)ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]],
                                Value = _adsReader.Current[Const.AdGroupFieldName],
                                OriginalID = _adsReader.Current[Const.AdGroupIdFieldName],
                                Status = adGroup_Status_Data[Convert.ToInt64(_adsReader.Current[Const.AdGroupIdFieldName])]
                            };

                            //Insert Network Type Display Network / Search Network
                            //string networkType = Convert.ToString(_adsReader.Current[Const.NetworkFieldName]);

                            //if (networkType.Equals(Const.GoogleSearchNetwork))
                            //    networkType = Const.SystemSearchNetwork;
                            //else if (networkType.Equals(Const.GoogleDisplayNetwork))
                            //    networkType = Const.SystemDisplayNetwork;

                            //ad.ExtraFields[NetworkType] = networkType;

                            importedAds.Add(adId, ad);
                            this.ImportManager.ImportAd(ad);
                        }
                        else ad = importedAds[adId];
                        adMetricsUnit.Ad = ad;

                        //SERACH KEYWORD IN KEYWORD/ Placements  Dictionary
                        KeywordPrimaryKey kwdKey = new KeywordPrimaryKey()
                        {
                            AdgroupId = Convert.ToInt64(_adsReader.Current[Const.AdGroupIdFieldName]),
                            KeywordId = Convert.ToInt64(_adsReader.Current[Const.KeywordIdFieldName]),
                            CampaignId = Convert.ToInt64(_adsReader.Current[Const.CampaignIdFieldName])
                        };

                        //Check if keyword file contains this kwdkey.
                        if (kwdKey.KeywordId != Convert.ToInt64(this.Delivery.Parameters["KeywordContentId"]) && _keywordsData.ContainsKey(kwdKey.ToString()))
                        {
                            KeywordTarget kwd = new KeywordTarget();
                            try
                            {
                                kwd = _keywordsData[kwdKey.ToString()];
                            }
                            catch (Exception)
                            {
                                //Creating KWD with OriginalID , since the KWD doesnt exists in KWD report.
                                kwd = new KeywordTarget { OriginalID = Convert.ToString(_adsReader.Current[Const.KeywordIdFieldName]) };
                            }

                            //INSERTING KEYWORD INTO METRICS
                            adMetricsUnit.TargetDimensions = new List<Target>();
                            adMetricsUnit.TargetDimensions.Add(kwd);
                        }
                        else
                        {
                            PlacementTarget placement = new PlacementTarget();
                            try
                            {
                                placement = _placementsData[kwdKey.ToString()];
                            }
                            catch (Exception)
                            {
                                placement.OriginalID = Convert.ToString(_adsReader.Current[Const.KeywordIdFieldName]);
                                placement.PlacementType = PlacementType.Automatic;
                                placement.Placement = Const.AutoDisplayNetworkName;
                            }
                            //INSERTING KEYWORD INTO METRICS
                            adMetricsUnit.TargetDimensions = new List<Target>();
                            adMetricsUnit.TargetDimensions.Add(placement);
                        }

                        //INSERTING METRICS DATA
                        adMetricsUnit.MeasureValues = new Dictionary<Measure, double>();
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Clicks], Convert.ToInt64(_adsReader.Current.Clicks));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Cost], (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000);
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Impressions], Convert.ToInt64(_adsReader.Current.Impressions));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.AveragePosition], Convert.ToDouble(_adsReader.Current[Const.AvgPositionFieldName]));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionOnePerClickFieldName]], Convert.ToDouble(_adsReader.Current[Const.ConversionOnePerClickFieldName]));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionManyPerClickFieldName]], Convert.ToDouble(_adsReader.Current[Const.ConversionManyPerClickFieldName]));

                        //Inserting conversion values
                        string conversionKey = String.Format("{0}#{1}", ad.OriginalID, _adsReader.Current[Const.KeywordIdFieldName]);
                        Dictionary<string, long> conversionDic = new Dictionary<string, long>();

                        //Dictionary<string, double> totalConvSum2 = new Dictionary<string, double>();

                        if (importedAdsWithConv.TryGetValue(conversionKey, out conversionDic))
                        {
                            foreach (var pair in conversionDic)
                            {
                                if (GoogleMeasuresDic.ContainsKey(pair.Key))
                                {
                                    adMetricsUnit.MeasureValues[this.ImportManager.Measures[GoogleMeasuresDic[pair.Key]]] = pair.Value;
                                }
                            }
                        }

                        adMetricsUnit.Currency = new Currency
                        {
                            Code = Convert.ToString(_adsReader.Current.Currency)
                        };
                        this.ImportManager.ImportMetrics(adMetricsUnit);
                    }

                    currentOutput.Checksum = _totals;
                    this.ImportManager.EndImport();
                }
                #endregion
            }
            return Core.Services.ServiceOutcome.Success;
        }




    }
    public static class Const
    {
        public const string AdPreRequiredHeader = "Keyword ID";
        public const string AutoPlacRequiredHeader = "Campaign ID";
        public const string EOF = "Total";

        public const string KeywordIdFieldName = "Keyword ID";
        public const string KeywordFieldName = "Keyword";
        public const string AvgPositionFieldName = "Avg. position";
        public const string KeywordStatusFieldName = "Keyword state";

        public const string ConversionManyPerClickFieldName = "Conv. (many-per-click)";
        public const string ConversionOnePerClickFieldName = "Conv. (1-per-click)";
        //public const string ConversionManyPerClick = "Conv. rate (many-per-click)";
        public const string TotalConversionsOnePerClickFieldName = "TotalConversionsOnePerClick";
        public const string ConversionTrackingPurposeFieldName = "Conversion tracking purpose";

        public const string AdGroupIdFieldName = "Ad group ID";
        public const string AdGroupFieldName = "Ad group";
        public const string AdGroupStatusFieldName = "Ad group state";

        public const string CampaignIdFieldName = "Campaign ID";
        public const string CampaignFieldName = "Campaign";
        public const string CampaignStatusFieldName = "Campaign state";

        public const string QualityScoreFieldName = "Quality score";
        public const string MatchTypeFieldName = "Match type";
        public const string PlacementFieldName = "Placement";
        public const string PlacementStatusFieldName = "Placement state";


        public const string AdIDFieldName = "Ad ID";
        public const string AdTypeFieldName = "Ad type";
        public const string AdDevicePreferenceFieldName = "Device Preference";
        public const string AdDevicePreferenceMobileFieldValue = "30001";
        public const string AdFieldName = "Ad";
        public const string AdStatusFieldName = "Ad state";
        public const string DisplayURLFieldName = "Display URL";
        public const string DestUrlFieldName = "Destination URL";

        public const string NetworkFieldName = "Network";
        public const string GoogleSearchNetworkFieldName = "Search Network";
        public const string SystemSearchNetworkFieldName = "Search Only";
        public const string GoogleDisplayNetworkFieldName = "Display Network";
        public const string SystemDisplayNetworkFieldName = "Content Only";

        public const string AutoDisplayNetworkName = "Total - content targeting";

        public const string DomainFieldName = "Domain";

        public static class AdTypeValues
        {
            public const string Text_ad = "Text ad";
            public const string Flash_ad = "Flash";
            public const string Image_ad = "Image ad";
            public const string Display_ad = "Display ad";
            public const string Product_listing_ad = "Product listing ad";
            public const string Mobile_ad = "Mobile ad";
            public const string Local_business_ad = "Local business ad";
            public const string Third_party_ad = "Third party ad";
            public const string Other = "Other";
            public const string Mobile_text = "Mobile Text ad";
            public const string Mobile_image = "Mobile Image ad";
            public const string Mobile_display = "Mobile Display ad";
        }

    }
}


