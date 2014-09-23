using System;
using System.Collections.Generic;
using Edge.Data.Pipeline.Services;
using Edge.Data.Pipeline;
using Edge.Data.Objects;
using GA = Google.Api.Ads.AdWords.v201406;
using System.IO;
using Edge.Data.Pipeline.Metrics.Services;
using Edge.Data.Pipeline.Metrics.AdMetrics;
using Edge.Data.Pipeline.Metrics;
using Edge.Data.Pipeline.Common.Importing;
using System.Linq;
using Edge.Core.Utilities;


namespace Edge.Services.Google.AdWords
{

    class ProcessorService : MetricsProcessorServiceBase
    {
        static ExtraField NetworkType = new ExtraField() { ColumnIndex = 1, Name = "NetworkType" };
        static ExtraField AdType = new ExtraField() { ColumnIndex = 2, Name = "adType" };
        static Dictionary<string, EdgeAdType> GoogleAdTypeDic;
        static Dictionary<string, string> GoogleMeasuresDic;
        static Dictionary<string, ObjectStatus> ObjectStatusDic;

        public enum EdgeAdType
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
            Mobile_display = 12,
            Sitelink = 14
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
            bool ConvertToUSD = Boolean.Parse(this.Delivery.Parameters["ConvertToUSD"].ToString());
            //double ConstCurrencyRate = this.Delivery.Parameters.ContainsKey("ConstCurrencyRate") ? Convert.ToDouble(this.Delivery.Parameters["ConstCurrencyRate"]) : 1;

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

                string[] requiredHeaders = new string[1];
                requiredHeaders[0] = Const.AdPreRequiredHeader;

                #region Getting Keywords Data
                Dictionary<string, double> _totals = new Dictionary<string, double>();
                DeliveryFile _keyWordsFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.KEYWORDS_PERFORMANCE_REPORT]];
                requiredHeaders[0] = Const.AdPreRequiredHeader;
                var _keywordsReader = new CsvDynamicReader(_keyWordsFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                _keywordsReader.MatchExactColumns = false;
                Dictionary<string, KeywordTarget> _keywordsData = new Dictionary<string, KeywordTarget>();

                this.ImportManager.BeginImport(this.Delivery);

                using (_keywordsReader)
                {
                    while (_keywordsReader.Read())
                    {
                        this.Mappings.OnFieldRequired = field => _keywordsReader.Current[field];

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
                            //Status = kwd_Status_Data[keywordPrimaryKey.ToString()]

                        };

                        keyword.QualityScore = Convert.ToString(_keywordsReader.Current[Const.QualityScoreFieldName]);
                        string matchType = _keywordsReader.Current[Const.MatchTypeFieldName];
                        keyword.MatchType = (KeywordMatchType)Enum.Parse(typeof(KeywordMatchType), matchType, true);

                        //Setting Tracker for Keyword
                        if (!String.IsNullOrWhiteSpace(Convert.ToString(_keywordsReader.Current[Const.DestUrlFieldName])))
                        {
                            keyword.DestinationUrl = Convert.ToString(_keywordsReader.Current[Const.DestUrlFieldName]);
                            //setting kwd tracker
                            //if (((String)(_keywordsReader.Current[Const.DestUrlFieldName])).IndexOf(Const.CreativeIDTrackingValue, StringComparison.OrdinalIgnoreCase) >= 0)
                            if (Convert.ToBoolean(this.Delivery.Parameters["UseKwdTrackerAsAdTracker"]))
                                if (this.Mappings != null && this.Mappings.Objects.ContainsKey(typeof(KeywordTarget)))
                                    this.Mappings.Objects[typeof(KeywordTarget)].Apply(keyword);
                        }



                        _keywordsData.Add(keywordPrimaryKey.ToString(), keyword);
                    }
                }
                #endregion

                Dictionary<string, PlacementTarget> _placementsData = new Dictionary<string, PlacementTarget>();



                #region Getting Placements Data


                string[] _placementsFileRequiredHeaders = new string[1];
                _placementsFileRequiredHeaders[0] = Const.PlacementCriteriaID;

                DeliveryFile _PlacementsFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEMENT_PERFORMANCE_REPORT]];
                var _PlacementsReader = new CsvDynamicReader(_PlacementsFile.OpenContents(compression: FileCompression.Gzip), _placementsFileRequiredHeaders);
                using (_PlacementsReader)
                {
                    while (_PlacementsReader.Read())
                    {
                        if (_PlacementsReader.Current[Const.PlacementCriteriaID] == Const.EOF)
                            break;

                        //Read data only if managed GDN, otherwise it is an automatic GDN so skip
                        if (!((String)(_PlacementsReader.Current[Const.PlacementCriteriaID])).Trim().Equals("--"))
                        {
                            KeywordPrimaryKey placementPrimaryKey = new KeywordPrimaryKey()
                            {
                                KeywordId = Convert.ToInt64(_PlacementsReader.Current[Const.PlacementCriteriaID]),
                                AdgroupId = Convert.ToInt64(_PlacementsReader.Current[Const.AdGroupIdFieldName]),
                                CampaignId = Convert.ToInt64(_PlacementsReader.Current[Const.CampaignIdFieldName])
                            };
                            PlacementTarget placement = new PlacementTarget()
                            {
                                OriginalID = _PlacementsReader.Current[Const.PlacementCriteriaID],
                                Placement = _PlacementsReader.Current[Const.PlacementFieldName],
                                PlacementType = PlacementType.Managed,
                                // Status = placement_kwd_Status_Data[placementPrimaryKey.ToString()]
                            };
                            //Setting Tracker for placment
                            if (!String.IsNullOrWhiteSpace(Convert.ToString(_PlacementsReader.Current[Const.DestUrlFieldName])))
                                placement.DestinationUrl = Convert.ToString(_PlacementsReader.Current[Const.DestUrlFieldName]);

                            _placementsData.Add(placementPrimaryKey.ToString(), placement);
                        }
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


                #region Getting Ads Data

                DeliveryFile _adPerformanceFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.AD_PERFORMANCE_REPORT]];
                var _adsReader = new CsvDynamicReader(_adPerformanceFile.OpenContents(compression: FileCompression.Gzip), requiredHeaders);
                Dictionary<string, Ad> importedAds = new Dictionary<string, Ad>();

                //session.Begin(false);
                //this.ImportManager.BeginImport(this.Delivery);

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

                        string currencyCode = ((string)(_adsReader.Current.Currency)).ToUpper();

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
                                    case Measure.Common.Cost: _totals[Measure.Common.Cost] =
                                        ConvertToUSD ? this.ConvertToUSD(this.Delivery.Parameters["CurrencyCode"].ToString().ToUpper(), (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000) : (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000;
                                        break;
                                    case Measure.Common.Impressions: _totals[Measure.Common.Impressions] = Convert.ToInt64(_adsReader.Current.Impressions); break;
                                }
                            }
                            break;
                        }

                        AdMetricsUnit adMetricsUnit = new AdMetricsUnit();
                        adMetricsUnit.Output = currentOutput;
                        Ad ad;

                        #region Try Get SearchKWD
                        /***********************************************************************************/

                        //Creating kwd primary key
                        KeywordPrimaryKey kwdKey = new KeywordPrimaryKey()
                        {
                            AdgroupId = Convert.ToInt64(_adsReader.Current[Const.AdGroupIdFieldName]),
                            KeywordId = Convert.ToInt64(_adsReader.Current[Const.KeywordIdFieldName]),
                            CampaignId = Convert.ToInt64(_adsReader.Current[Const.CampaignIdFieldName])
                        };

                        //Check if keyword file contains this kwdkey and not a GDN Keyword
                        String[] GdnKwdIds = this.Delivery.Parameters["KeywordContentId"].ToString().Split(',');
                        bool IsSearchKwd = false;
                        KeywordTarget kwd = null;
                        PlacementTarget placement = null;

                        if (!GdnKwdIds.Contains(kwdKey.KeywordId.ToString()) && _keywordsData.ContainsKey(kwdKey.ToString()))
                        {
                            kwd = new KeywordTarget();

                            try
                            {
                                kwd = _keywordsData[kwdKey.ToString()];

                            }
                            catch (Exception)
                            {
                                //Creating KWD with OriginalID , since the KWD doesnt exists in KWD report.
                                kwd = new KeywordTarget { OriginalID = Convert.ToString(_adsReader.Current[Const.KeywordIdFieldName]) };
                            }

                            IsSearchKwd = true;
                        }
                        else
                        {
                            placement = new PlacementTarget();
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
                        }
                        /***********************************************************************************/
                        #endregion

                        string adId = _adsReader.Current[Const.AdIDFieldName];
                        if (!importedAds.ContainsKey(adId))
                        {
                            ad = new Ad();
                            ad.OriginalID = adId;
                            ad.Channel = new Channel() { ID = 1 };
                            ad.Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_adPerformanceFile.Parameters["AdwordsClientID"] };
                            // ad.Status = ad_Status_Data[Convert.ToInt64(adId)];

                            #region Ad Type
                            /****************************************************************/
                            string adTypeColumnValue = Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]);
                            string devicePreferenceColumnValue = Convert.ToString(_adsReader.Current[Const.AdDevicePreferenceFieldName]);
                            string adTypeEdgeValue = GoogleAdTypeDic[adTypeColumnValue].ToString();

                            //EdgeAdType atv = (EdgeAdType)Enum.Parse(typeof(EdgeAdType), adTypeEdgeValue, true);

                            //is mobile ad ? 
                            if (devicePreferenceColumnValue.Equals(Const.AdDevicePreferenceMobileFieldValue))
                            {
                                string mobileValue = string.Format("Mobile {0}", Convert.ToString(_adsReader.Current[Const.AdTypeFieldName]));

                                //Check if this mobile value exists on dictionary
                                if (GoogleAdTypeDic.ContainsKey(mobileValue))
                                    adTypeEdgeValue = GoogleAdTypeDic[mobileValue].ToString();

                                else adTypeEdgeValue = GoogleAdTypeDic[Const.AdTypeValues.Mobile_ad].ToString();
                            }

                            ad.ExtraFields[AdType] = (int)(EdgeAdType)Enum.Parse(typeof(EdgeAdType), adTypeEdgeValue, true); ;
                            /****************************************************************/
                            #endregion


                            //Creative
                            ad.Creatives.Add(new TextCreative { TextType = TextCreativeType.DisplayUrl, Text = _adsReader.Current[Const.DisplayURLFieldName] });

                            #region Ad Tracker segment
                            /******************************************************/
                            ////Setting Tracker for Ad
                            if (!String.IsNullOrWhiteSpace(_adsReader.Current[Const.DestUrlFieldName]))
                            {
                                ad.DestinationUrl = _adsReader.Current[Const.DestUrlFieldName];

                                if (this.Mappings != null && this.Mappings.Objects.ContainsKey(typeof(Ad)))
                                    this.Mappings.Objects[typeof(Ad)].Apply(ad);
                            }

                            //if Ad doesnt contains tracker than check for kwd tracker
                            if (Convert.ToBoolean(this.Delivery.Parameters["UseKwdTrackerAsAdTracker"]))
                                if (kwd != null && kwd.Segments != null && kwd.Segments.ContainsKey(this.ImportManager.SegmentTypes[Segment.Common.Tracker]))
                                    if (kwd.Segments[this.ImportManager.SegmentTypes[Segment.Common.Tracker]] != null && kwd.Segments[this.ImportManager.SegmentTypes[Segment.Common.Tracker]].Value != null)
                                    {
                                        SegmentObject tracker = kwd.Segments[this.ImportManager.SegmentTypes[Segment.Common.Tracker]];

                                        //if value contains ADID than replace ADID with AD original id
                                        tracker.Value = tracker.Value.Replace("ADID", ad.OriginalID);
                                        ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Tracker]] = tracker;

                                    }
                            /******************************************************/
                            #endregion

                            #region Campaign
                            /****************************************************************/
                            ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]] = new Campaign()
                            {
                                OriginalID = _adsReader.Current[Const.CampaignIdFieldName],
                                Name = _adsReader.Current[Const.CampaignFieldName],
                                //Status = campaign_Status_Data[Convert.ToInt64(_adsReader.Current[Const.CampaignIdFieldName])]

                            };
                            /****************************************************************/
                            #endregion

                            #region Image
                            /****************************************************************/
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
                            /****************************************************************/
                            #endregion

                            #region Text od Display
                            /****************************************************************/
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
                            /****************************************************************/
                            #endregion

                            #region Adgroup
                            /****************************************************************/
                            //Insert Adgroup 
                            ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.AdGroup]] = new AdGroup()
                            {
                                Campaign = (Campaign)ad.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]],
                                Value = _adsReader.Current[Const.AdGroupFieldName],
                                OriginalID = _adsReader.Current[Const.AdGroupIdFieldName],
                                // Status = adGroup_Status_Data[Convert.ToInt64(_adsReader.Current[Const.AdGroupIdFieldName])]
                            };
                            /****************************************************************/
                            #endregion

                            #region Network
                            /****************************************************************/
                            //Insert Network Type Display Network / Search Network
                            //string networkType = Convert.ToString(_adsReader.Current[Const.NetworkFieldName]);

                            //if (networkType.Equals(Const.GoogleSearchNetwork))
                            //    networkType = Const.SystemSearchNetwork;
                            //else if (networkType.Equals(Const.GoogleDisplayNetwork))
                            //    networkType = Const.SystemDisplayNetwork;

                            //ad.ExtraFields[NetworkType] = networkType;
                            /****************************************************************/
                            #endregion

                            importedAds.Add(adId, ad);
                            this.ImportManager.ImportAd(ad);
                        }
                        else ad = importedAds[adId];

                        adMetricsUnit.Ad = ad;

                        //INSERTING SEARCH KEYWORD INTO METRICS
                        if (IsSearchKwd & kwd != null)
                        {
                            adMetricsUnit.TargetDimensions = new List<Target>();
                            adMetricsUnit.TargetDimensions.Add(kwd);
                        }
                        //INSERTING GDN KEYWORD INTO METRICS
                        else if (placement != null)
                        {
                            //INSERTING KEYWORD INTO METRICS
                            adMetricsUnit.TargetDimensions = new List<Target>();
                            adMetricsUnit.TargetDimensions.Add(placement);
                        }

                        //INSERTING METRICS DATA
                        adMetricsUnit.MeasureValues = new Dictionary<Measure, double>();
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Clicks], Convert.ToInt64(_adsReader.Current.Clicks));

                        //Currencies Conversion Support
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Cost],
                            ConvertToUSD ? this.ConvertToUSD(currencyCode.ToUpper(), (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000) : (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000);
                        if (ConvertToUSD)
                        {
                            adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.CostBeforeConversion], (Convert.ToDouble(_adsReader.Current.Cost)) / 1000000);
                            adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.USDConversionRate], Convert.ToDouble(this.ImportManager.CurrencyRates[currencyCode].RateValue));
                        }
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Impressions], Convert.ToInt64(_adsReader.Current.Impressions));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.AveragePosition], Convert.ToDouble(_adsReader.Current[Const.AvgPositionFieldName]));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionOnePerClickFieldName]], Convert.ToDouble(_adsReader.Current[Const.ConversionOnePerClickFieldName]));
                        adMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionManyPerClickFieldName]], Convert.ToDouble(_adsReader.Current[Const.ConversionManyPerClickFieldName]));

                        //Inserting conversion values
                        string conversionKey = String.Format("{0}#{1}", ad.OriginalID, _adsReader.Current[Const.KeywordIdFieldName]);
                        Dictionary<string, long> conversionDic = new Dictionary<string, long>();



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

                #endregion
                }

                #region Getting Sitelinks Data without metrics

                if (this.Delivery.Parameters.ContainsKey("AppendSitelinks") && Boolean.Parse(this.Delivery.Parameters["AppendSitelinks"].ToString()))
                {

                    string[] sitelinksRequiredHeaders = new string[1];
                    sitelinksRequiredHeaders[0] = Const.PlaceholderFeedItemID;

                    DeliveryFile _sitelinkPerformanceFile = this.Delivery.Files[GoogleStaticReportsNamesUtill._reportNames[GA.ReportDefinitionReportType.PLACEHOLDER_FEED_ITEM_REPORT]];
                    var _sitelinkReader = new CsvDynamicReader(_sitelinkPerformanceFile.OpenContents(compression: FileCompression.Gzip), sitelinksRequiredHeaders);

                    AdMetricsUnit siteLinkMetricsUnit = new AdMetricsUnit();
                    siteLinkMetricsUnit.Output = currentOutput;
                    Ad sitelinkAd;

                    using (_sitelinkReader)
                    {
                        this.Mappings.OnFieldRequired = field => _sitelinkReader.Current[field];
                        //to do : get site link tracker
                        string[] sitelinkAttr;

                        while (_sitelinkReader.Read())
                        {

                            if (((String)_sitelinkReader.Current[Const.SiteLinkAttributeValuesHeader]).Equals(Const.Sitelink_EOF))
                                break;

                            string sitelinkId = string.Format("{0}{1}{2}", _sitelinkReader.Current[Const.PlaceholderFeedItemID], _sitelinkReader.Current[Const.CampaignIdFieldName], _sitelinkReader.Current[Const.AdGroupIdFieldName]);
                            sitelinkAd = new Ad();
                            sitelinkAd.OriginalID = sitelinkId;
                            sitelinkAd.Channel = new Channel() { ID = 1 };
                            sitelinkAd.Account = new Account { ID = this.Delivery.Account.ID, OriginalID = (String)_adPerformanceFile.Parameters["AdwordsClientID"] };


                            //Creative

                            sitelinkAttr = ((String)_sitelinkReader.Current[Const.SiteLinkAttributeValuesHeader]).Split(';');

                            bool legacy = sitelinkAttr.Count() != 4 ? true : false;

                            string destUrl = string.Empty;
                            bool destUrlParsingError = false;

                            //Checking desturl errors ( we would like to insert only sitelinks that contains desturl ).
                            try
                            {
                                destUrl = sitelinkAttr[1];
                            }
                            catch (Exception e)
                            {
                                Log.Write("Error while trying to pars destination url from attribute field on sitelink report", e);
                                destUrlParsingError = true;

                            }
                            if (!destUrlParsingError)
                            {

                                ////Setting Tracker for Ad
                                if (!String.IsNullOrWhiteSpace(sitelinkAttr[1]))
                                {
                                    sitelinkAd.DestinationUrl = sitelinkAttr[1];

                                    if (this.Mappings != null && this.Mappings.Objects.ContainsKey(typeof(Ad)))
                                        this.Mappings.Objects[typeof(Ad)].Apply(sitelinkAd);
                                }

                                sitelinkAd.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]] = new Campaign()
                                {
                                    OriginalID = _sitelinkReader.Current[Const.CampaignIdFieldName],
                                    Name = _sitelinkReader.Current[Const.CampaignFieldName],
                                };

                                //Insert Adgroup 
                                sitelinkAd.Segments[this.ImportManager.SegmentTypes[Segment.Common.AdGroup]] = new AdGroup()
                                {
                                    Campaign = (Campaign)sitelinkAd.Segments[this.ImportManager.SegmentTypes[Segment.Common.Campaign]],
                                    Value = _sitelinkReader.Current[Const.AdGroupFieldName],
                                    OriginalID = _sitelinkReader.Current[Const.AdGroupIdFieldName],
                                    // Status = adGroup_Status_Data[Convert.ToInt64(_adsReader.Current[Const.AdGroupIdFieldName])]
                                };

                                sitelinkAd.Creatives.Add(new TextCreative { TextType = TextCreativeType.DisplayUrl, Text = sitelinkAttr[1] });

                                if (!legacy) // only in case it doesnt contains legacy siteink
                                    sitelinkAd.Creatives.Add(new TextCreative
                                    {
                                        TextType = TextCreativeType.Body,
                                        Text = sitelinkAttr[2],
                                        Text2 = sitelinkAttr[3]
                                    });

                                sitelinkAd.Name = "[Sitelink] " + sitelinkAttr[0];
                                sitelinkAd.Creatives.Add(new TextCreative
                                {
                                    TextType = TextCreativeType.Title,
                                    Text = "[Sitelink] " + sitelinkAttr[0]
                                });

                                //Ad Type
                                //Note: changed to "sitelink" following Amir request
                                sitelinkAd.ExtraFields[AdType] = (int)(EdgeAdType.Sitelink);


                                siteLinkMetricsUnit.Ad = sitelinkAd;

                                //Setting Default Currency as USD following Amir's request from March 2014
                                siteLinkMetricsUnit.Currency = new Currency
                                {
                                    Code = "USD"
                                };

                                //INSERTING METRICS DATA
                                siteLinkMetricsUnit.MeasureValues = new Dictionary<Measure, double>();
                                siteLinkMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Clicks], 0);
                                siteLinkMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Cost], 0);
                                siteLinkMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.Impressions], 0);
                                siteLinkMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[Measure.Common.AveragePosition], 0);
                                siteLinkMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionOnePerClickFieldName]], 0);
                                siteLinkMetricsUnit.MeasureValues.Add(this.ImportManager.Measures[GoogleMeasuresDic[Const.ConversionManyPerClickFieldName]], 0);
                                ImportManager.ImportMetrics(siteLinkMetricsUnit);
                                ImportManager.ImportAd(sitelinkAd);

                            }

                        }
                    }
                }// end if 
                #endregion


                currentOutput.Checksum = _totals;
                this.ImportManager.EndImport();


            }
            return Core.Services.ServiceOutcome.Success;
        }




    }
    public static class Const
    {
        public const string SiteLinkAttributeValuesHeader = "Attribute Values";
        public const string PlaceholderFeedID = "Feed ID";
        public const string PlaceholderFeedItemID = "Feed item ID";
        public const string SitelinkAdgroupName = "SiteLink";

        public const string AdPreRequiredHeader = "Keyword ID";
        public const string AutoPlacRequiredHeader = "Campaign ID";
        public const string EOF = "Total";

        public const string KeywordIdFieldName = "Keyword ID";
        public const string KeywordFieldName = "Keyword";
        public const string AvgPositionFieldName = "Avg. position";
        public const string KeywordStatusFieldName = "Keyword state";
        public const string PlacementCriteriaID = "Criterion ID";

        public const string ConversionManyPerClickFieldName = "Conversions";
        public const string ConversionOnePerClickFieldName = "Converted clicks";
        //public const string ConversionManyPerClick = "Click conversion rate";
        public const string TotalConversionsOnePerClickFieldName = "Total conv. value";
        public const string ConversionTrackingPurposeFieldName = "Conversion category";

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
        public const string AdDevicePreferenceFieldName = "Device preference";
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


        public const string Sitelink_EOF = "--";
        public const string CreativeIDTrackingValue = "{creative}";
    }
}


