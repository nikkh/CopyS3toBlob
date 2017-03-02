using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Amazon.S3;
using Amazon.S3.Model;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO.Compression;
using Newtonsoft.Json;

namespace CopyS3toBlob
{
    // To learn more about Microsoft Azure WebJobs SDK, please see http://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        static IAmazonS3 client;
        static Dictionary<string, S3Object> files;
        static string azureStorageContainerName;
       
        static int count;
        static CloudBlobClient azureClient;
        static CloudStorageAccount azureStorageAccount;
      

        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
       
        private static void Main()
        {


            #region Parse Configuration Settings
            AwsSettings awsSettings = new AwsSettings();
            string AWS_SETTINGS = CloudConfigurationManager.GetSetting("AWS_SETTINGS");
            awsSettings = JsonConvert.DeserializeObject<AwsSettings>(AWS_SETTINGS);
            azureStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureStorageConnectionString"));
            azureStorageContainerName = CloudConfigurationManager.GetSetting("AzureStorageContainerName");
            #endregion

            Console.WriteLine("CopyS3toBlob: {0}.", awsSettings.Description);

            

            // For each account in AwsSettings
            foreach (var account in awsSettings.Accounts)
            {
                ConsoleColor currentColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine(@"Processing AWS Account {0}", account);
                Console.ForegroundColor = currentColor;

                // For each bucket we need to inspect for this account
                foreach (var bucket in account.Buckets)
                {
                    Console.WriteLine(@"Processing Bucket {0}", bucket.Name);
                    count = 0;
                    files = new Dictionary<string, S3Object>();
                    Amazon.RegionEndpoint region = GetAwsRegionFromString(bucket.Region);
                    using (client = new AmazonS3Client(account.AccessKey, account.SecretAccessKey, region))
                    {

                        try
                        {
                            ListObjectsV2Request request = new ListObjectsV2Request
                            {
                                BucketName = bucket.Name,
                                MaxKeys = 10,
                                Prefix = account.AccountId
                            };
                            ListObjectsV2Response response;
                            do
                            {
                                response = client.ListObjectsV2(request);

                                // Process response.
                                foreach (S3Object entry in response.S3Objects)
                                {
                                   
                                    if (FileIsRequired(entry.Key, bucket.Patterns))
                                    {
                                        count++;
                                        //Console.WriteLine("key = {0} size = {1}, Date={2}", entry.Key, entry.Size, entry.LastModified);
                                        S3Object theFile = new S3Object();
                                        theFile.BucketName = entry.BucketName;
                                        theFile.ETag = entry.ETag;
                                        theFile.LastModified = entry.LastModified;
                                        theFile.Owner = entry.Owner;
                                        theFile.Size = entry.Size;
                                        theFile.StorageClass = entry.StorageClass;
                                        files.Add(entry.Key, theFile);
                                    }


                                }

                                request.ContinuationToken = response.NextContinuationToken;
                            } while (response.IsTruncated == true);
                            
                        }
                        catch (AmazonS3Exception amazonS3Exception)
                        {
                            if (amazonS3Exception.ErrorCode != null &&
                                (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId")
                                ||
                                amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                            {
                                Console.WriteLine("Check the provided AWS Credentials.");
                                Console.WriteLine(
                                "To sign up for service, go to http://aws.amazon.com/s3");
                            }
                            else
                            {
                                Console.WriteLine(
                                 "Error occurred. Message:'{0}' when listing objects",
                                 amazonS3Exception.Message);
                            }
                            throw amazonS3Exception;
                        }

                    }
                    ConsoleColor savedColour = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Bucket{0} contains {1} files of interest:",
                        bucket.Name, count);
                    Console.ForegroundColor = savedColour;
                    BucketBlobPackage package = new BucketBlobPackage();
                    package.AccountId = account.AccountId;
                    package.AccessKey = account.AccessKey;
                    package.SecretAccessKey = account.SecretAccessKey;
                    package.BucketInfo = bucket;
                    package.FileInfo = files;
                    WriteToBlobStorage(package);
                }


            }

            
            Console.WriteLine("CopyS2toBlob job has completed sucessfully");
            Console.ReadKey();
        }

        private static bool FileIsRequired(string fileName, List<string> patterns)
        {
            bool retval = false;
            foreach (var pattern in patterns)
            {
                if (fileName.Contains(pattern))
                {
                    retval = true;
                    break;
                }
            }
            return retval;
        }

        private static Amazon.RegionEndpoint GetAwsRegionFromString(string region)
        {

            switch (region)
            {
                case "EUWest2":
                    return Amazon.RegionEndpoint.EUWest2;
                case "EUWest1":
                    return Amazon.RegionEndpoint.EUWest1;
                default:
                    throw new Exception("Unsupported region found in AwsSettings");
            }
        }

        private static void WriteToBlobStorage(BucketBlobPackage package)
        {

            azureClient = azureStorageAccount.CreateCloudBlobClient();

            foreach (var item in package.FileInfo)
            {
                
                using (client = new AmazonS3Client(package.AccessKey, package.SecretAccessKey, GetAwsRegionFromString(package.BucketInfo.Region)))
                {
                    GetObjectRequest request = new GetObjectRequest
                    {
                        BucketName = package.BucketInfo.Name,
                        Key = item.Key
                    };

                    using (GetObjectResponse response = client.GetObject(request))
                    {
                        using (Stream responseStream = response.ResponseStream)

                        {
                            string fileExtension = item.Key.Substring(Math.Max(0, item.Key.Length - 4)).ToUpper();
                            switch (fileExtension)
                            {
                               
                                case ".ZIP":
                                    CopyZIPtoBlob(responseStream);
                                     break;
                                default:
                                    S3Object o = item.Value as S3Object;
                                    CopySingleFileToBlob(responseStream, item.Key, o.Size);
                                    break;
                            }

 


                        }

                    }
                }
                
                

            }

          
        }

    

        private static void CopySingleFileToBlob(Stream responseStream, string name, long size)
        {
            
                    using (responseStream)
                    {
                        var blob = azureClient.GetContainerReference(azureStorageContainerName).GetBlockBlobReference(name);
                        if (!blob.Exists())
                        {
                            blob.UploadFromStream(responseStream);
                            ConsoleColor savedColour = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("Blob {0} did not exist and was created", blob.Uri.ToString());
                            Console.ForegroundColor = savedColour;
                  
                        }
                        else
                        {
                            if (blob.Properties.Length != size)
                            {
                                blob.Delete();
                                blob.UploadFromStream(responseStream);
                                ConsoleColor savedColour = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine("Blob {0} was a different size and was refreshed", blob.Uri); ;
                                Console.ForegroundColor = savedColour;
                                
                            }
                            else
                            {
                                ConsoleColor savedColour = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.DarkGray;
                                Console.WriteLine("{0} is identical and will skipped", name);
                                Console.ForegroundColor = savedColour;
                                ;
                            }
                        }

                    }
                }

          
        

        private static void CopyZIPtoBlob(Stream responseStream)
        {
            using (ZipArchive archive = new ZipArchive(responseStream, ZipArchiveMode.Read))
            {
               
                if (archive.Entries.Count == 0)
                {
                    Console.WriteLine("There were no entries in archive!");
                }
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    //Console.WriteLine("{0} was discovered in the archive", entry.Name);
                    using (Stream entryStream = entry.Open())
                    {
                        CopySingleFileToBlob(entryStream, entry.Name, entry.Length);
                    }
                }

            }
        }
    }
}
