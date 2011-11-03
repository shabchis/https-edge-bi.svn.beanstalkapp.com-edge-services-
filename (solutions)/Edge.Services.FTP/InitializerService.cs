﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Edge.Data.Pipeline;
using Edge.Data.Pipeline.Services;
using Edge.Core.Services;
using System.Net;
using System.IO;

namespace Edge.Services.FTP
{
    public class InitializerService : PipelineService
    {
        protected override ServiceOutcome DoPipelineWork()
        {
            this.Delivery = this.NewDelivery(); // setup delivery

            //Get Ftp Url
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["FtpServer"]))
                throw new Exception("Missing Configuration Param , FtpServer");
            string FtpServer = this.Instance.Configuration.Options["FtpServer"];


            //Get AllowedExtensions
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["AllowedExtensions"]))
                throw new Exception("Missing Configuration Param , AllowedExtensions");
            string[] AllowedExtensions = this.Instance.Configuration.Options["AllowedExtensions"].Split('|');


            //Get Permissions
            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["UserID"]))
                throw new Exception("Missing Configuration Param , UserID");
            string UserId = this.Instance.Configuration.Options["UserID"];


            if (String.IsNullOrEmpty(this.Instance.Configuration.Options["Password"]))
                throw new Exception("Missing Configuration Param , Password");
            string Password = Core.Utilities.Encryptor.Dec(this.Instance.Configuration.Options["Password"]);


            this.Delivery.TargetLocationDirectory = Instance.Configuration.Options["DeliveryFilesDir"];
            if (string.IsNullOrEmpty(this.Delivery.TargetLocationDirectory))
                throw new Exception("Delivery.TargetLocationDirectory must be configured in configuration file (DeliveryFilesDir)");
            this.Delivery.TargetPeriod = this.TargetPeriod;
            this.Delivery.Account = new Edge.Data.Objects.Account() { ID = this.Instance.AccountID, };
            this.Delivery.Channel = new Data.Objects.Channel() { ID = 9999 };

            this.Delivery.Signature = Delivery.CreateSignature(String.Format("FTP-[{0}]-[{1}]-[{2}]]",
                this.Instance.AccountID,this.Delivery.Channel.ToString(),this.TargetPeriod.ToAbsolute()));

            //Getting files in ftp directory
            FtpWebRequest request;
            List<string> files = new List<string>();
            try
            {
                request = (FtpWebRequest)FtpWebRequest.Create(new Uri(FtpServer + "/"));
                request.UseBinary = true;
                request.Credentials = new NetworkCredential(UserId, Password);
                request.Method = WebRequestMethods.Ftp.ListDirectory;

                WebResponse response = request.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream());
                string file = reader.ReadLine();
                bool ExtensionsFlag = false;

                while (file != null)
                {
                    //Checking AllowedExtensions
                    string[] fileExtension = file.Split('.');
                    foreach (string item in AllowedExtensions)
                    {
                        if (fileExtension[fileExtension.Length-1].ToLower().Equals(item.ToLower()))
                            continue;
                        ExtensionsFlag = true;
                    }
                    if (!ExtensionsFlag) files.Add(file); //Get only matched extension files
                    file = reader.ReadLine();
                }
                reader.Close();
                response.Close();

                if (files.Count == 0)
                    Core.Utilities.Log.Write("No files in FTP directory for account id " + this.Instance.AccountID.ToString(), Core.Utilities.LogMessageType.Information);
                else
                    //creating Delivery File foreach file in ftp
                    foreach (string fileName in files)
                    {
                        this.Delivery.Files.Add(new Data.Pipeline.DeliveryFile()
                        {
                            Name = "FTP_" + fileName,
                            SourceUrl = FtpServer + "/" + fileName,
                        }
                        );
                    }
            }
            catch (Exception e)
            {
                Core.Utilities.Log.Write(
                    string.Format("Cannot connect FTP server for account ID:{0}  Exception: {1}",
                    this.Instance.AccountID.ToString(), e.Message),
                    Core.Utilities.LogMessageType.Information);
            }



            this.Delivery.Parameters["FtpServer"] = FtpServer;
            this.Delivery.Parameters["AllowedExtensions"] = AllowedExtensions;
            this.Delivery.Parameters["UserID"] = UserId;
            this.Delivery.Parameters["Password"] = Password;
            this.Delivery.Save();
            return Core.Services.ServiceOutcome.Success;
        }

    }
}
