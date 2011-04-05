﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceModel;
using System.Text;
using Edge.Core;
using Edge.Core.Services;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Configuration;
using WS = Edge.Services.Microsoft.AdCenter.ServiceReferences.V7.ReportingService;
using Edge.Data.Pipeline.Readers;

namespace Edge.Services.Microsoft.AdCenter
{
	public class AdCenterApi
	{
		PipelineService _service;

		public AdCenterApi(PipelineService service)
		{
			_service = service;
		}

		public WS.KeywordPerformanceReportRequest NewKeywordPerformanceReportRequest(params WS.KeywordPerformanceReportColumn[] columns)
		{
			// Create the nested report request
			var request = new WS.KeywordPerformanceReportRequest()
			{
				Language = WS.ReportLanguage.English,
				Format = WS.ReportFormat.Xml,
				ReturnOnlyCompleteData = false,
				ReportName = String.Format("KeywordPerformance (delivery: {0})", _service.Delivery.DeliveryID),
				Aggregation = WS.ReportAggregation.Daily,
				Time = ConvertToReportTime(_service.TargetPeriod),
				Columns = columns,
				Scope = new WS.AccountThroughAdGroupReportScope()
				{
					AccountIds = new long[] { long.Parse(_service.Instance.Configuration.Options["AdCenter.CustomerAccountID"]) }
				}
			};

			return request;
		}

		public WS.AdPerformanceReportRequest NewAdPerformanceReportRequest(params WS.AdPerformanceReportColumn[] columns)
		{
			// Create the nested report request
			var request = new WS.AdPerformanceReportRequest()
			{
				Language = WS.ReportLanguage.English,
				Format = WS.ReportFormat.Xml,
				ReturnOnlyCompleteData = false,
				ReportName = String.Format("AdPerformance (delivery: {0})", _service.Delivery.DeliveryID),
				Aggregation = WS.NonHourlyReportAggregation.Daily,
				Time = ConvertToReportTime(_service.TargetPeriod),
				Columns = columns,
				Scope = new WS.AccountThroughAdGroupReportScope()
				{
					AccountIds = new long[] { long.Parse(_service.Instance.Configuration.Options["AdCenter.CustomerAccountID"]) }
				}
			};

			return request;
		}

		public string SubmitReportRequest(WS.ReportRequest request)
		{
			// Create the API request
			var submitRequest = new WS.SubmitGenerateReportRequest()
			{
				ApplicationToken = _service.Instance.Configuration.Options["AdCenter.AppToken"],
				DeveloperToken = _service.Instance.Configuration.Options["AdCenter.DevToken"],
				UserName = _service.Instance.Configuration.Options["AdCenter.Username"],
				Password = _service.Instance.Configuration.Options["AdCenter.Password"],
				CustomerId = _service.Instance.Configuration.Options["AdCenter.CustomerID"],
				CustomerAccountId = _service.Instance.Configuration.Options["AdCenter.CustomerAccountID"],
				ReportRequest = request
			};

			// Open a connection
			using (var service = new WS.ReportingServiceClient())
			{
				try
				{
					// Submit the report request
					WS.SubmitGenerateReportResponse queueResponse = service.SubmitGenerateReport(submitRequest);

					// Poll to get the status of the report until it is complete.
					TimeSpan interval = _service.Instance.Configuration.Options["AdCenter.PollInterval"] != null ?
						TimeSpan.Parse(_service.Instance.Configuration.Options["AdCenter.PollInterval"]) :
						TimeSpan.FromMinutes(1);

					var pollRequest = new WS.PollGenerateReportRequest()
					{
						ApplicationToken = submitRequest.ApplicationToken,
						DeveloperToken = submitRequest.DeveloperToken,
						UserName = submitRequest.UserName,
						Password = submitRequest.Password,
						ReportRequestId = queueResponse.ReportRequestId
					};

					WS.PollGenerateReportResponse pollResponse = null;
					do
					{
						// Wait the specified number of minutes before polling.
						System.Threading.Thread.Sleep(interval);

						// Get the status of the report.
						pollResponse = service.PollGenerateReport(pollRequest);

						if (pollResponse.ReportRequestStatus.Status == WS.ReportRequestStatusType.Success)
						{
							// The report is ready.
							break;
						}
						else if (pollResponse.ReportRequestStatus.Status == WS.ReportRequestStatusType.Pending)
						{
							// The report is not ready yet.
							continue;
						}
						else
						{
							// An error occurred.
							break;
						}
					}
					while (true);

					// If the report was created, return the download URL
					if (pollResponse != null && pollResponse.ReportRequestStatus.Status == WS.ReportRequestStatusType.Success)
					{
						return pollResponse.ReportRequestStatus.ReportDownloadUrl;
					}
					else
					{
						throw new Exception(String.Format("Report request status came back as '{0}' but no exception was thrown (tracking ID: {1}).",
							pollResponse.ReportRequestStatus.Status,
							pollResponse.TrackingId
							));
					}
				}

				catch (FaultException<WS.AdApiFaultDetail> fault)
				{
					// Format error details into exception message
					string msg = String.Format("Ad API error occured (tracking ID: {0}):\n", fault.Detail.TrackingId);
					foreach (WS.AdApiError error in fault.Detail.Errors)
					{
						msg += String.Format("\t{0} (#{1}) - {2}{3}\n",
							error.ErrorCode,
							error.Code,
							error.Message,
							error.Detail != null ? '{' + error.Detail + '}' : null
						);
					}
					throw new Exception(msg);
				}

				catch (FaultException<WS.ApiFaultDetail> fault)
				{
					// Format error details into exception message
					string msg = String.Format("General API error occured (tracking ID: {0}):\n", fault.Detail.TrackingId);

					// operation errors
					if (fault.Detail.OperationErrors.Length > 0)
					{
						msg += "Operation errors\n:";
						foreach (WS.OperationError error in fault.Detail.OperationErrors)
						{
							msg += String.Format("\t{0} (#{1}) - {2}{3}\n",
								error.ErrorCode,
								error.Code,
								error.Message,
								error.Details != null ? '(' + error.Details + ')' : null
							);
						}
					}

					// batch errors (probably not relevant here)
					if (fault.Detail.BatchErrors.Length > 0)
					{
						msg += "Batch errors\n:";
						foreach (WS.BatchError error in fault.Detail.BatchErrors)
						{
							msg += String.Format("\tItem #{4}: {0} (#{1}) - {2}{3}\n",
								error.ErrorCode,
								error.Code,
								error.Message,
								error.Details != null ? '{' + error.Details + '}' : null,
								error.Index
							);
						}
					}

					throw new Exception(msg);
				}
			}
		}

		public static WS.ReportTime ConvertToReportTime(DateTimeRange range)
		{
			DateTime startTime = range.Start.ToDateTime();
			DateTime endTime = range.End.ToDateTime();

			return new WS.ReportTime()
			{
				CustomDateRangeStart = new WS.Date()
				{
					Year = startTime.Year,
					Month = startTime.Month,
					Day = startTime.Day
				},
				CustomDateRangeEnd = new WS.Date()
				{
					Year = endTime.Year,
					Month = endTime.Month,
					Day = endTime.Day
				}
			};
		}

		public static string GetTimePeriodColumnName(WS.ReportAggregation aggregation)
		{
			string column = null;
			switch (aggregation)
			{
				case WS.ReportAggregation.Daily: column = "GregorianDate"; break;
				case WS.ReportAggregation.Hourly: column = "Hour"; break;
				case WS.ReportAggregation.Monthly: column = "MonthStartDate"; break;
				case WS.ReportAggregation.Weekly: column = "WeekStartDate"; break;
				case WS.ReportAggregation.Yearly: column = "Year"; break;
			}
			return column;
		}

		public static string GetTimePeriodColumnName(WS.NonHourlyReportAggregation aggregation)
		{
			string column = null;
			switch (aggregation)
			{
				case WS.NonHourlyReportAggregation.Daily: column = "GregorianDate"; break;
				case WS.NonHourlyReportAggregation.Monthly: column = "MonthStartDate"; break;
				case WS.NonHourlyReportAggregation.Weekly: column = "WeekStartDate"; break;
				case WS.NonHourlyReportAggregation.Yearly: column = "Year"; break;
			}
			return column;
		}
	}
}
