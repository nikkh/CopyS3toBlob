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

namespace CopyS3toBlob
{
    // To learn more about Microsoft Azure WebJobs SDK, please see http://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        static IAmazonS3 client;
        static Dictionary<string, S3Object> files;
        static string bucketName = "nh-usage-1";
        static string azureStorageContainerName = "awsreports";
        const string fileIdentifier = "aws-billing-detailed-line-items-with-resources-and-tags";
        static int count;
        
        static CloudBlobClient azureClient;
        static CloudStorageAccount azureStorageAccount;

        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        static void Main()
        {
            
            files = new Dictionary<string, S3Object>();
            Console.WriteLine("CopyS3toBlob job has started...");
            using (client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1))
            {
                Console.WriteLine("Listing objects stored in bucket: "+bucketName);
                count = 0;

                try
                {
                    ListObjectsV2Request request = new ListObjectsV2Request
                    {
                        BucketName = bucketName,
                        MaxKeys = 10
                    };
                    ListObjectsV2Response response;
                    do
                    {
                        response = client.ListObjectsV2(request);
                       
                        // Process response.
                        foreach (S3Object entry in response.S3Objects)
                        {
                            if (entry.Key.Contains(fileIdentifier))
                            {
                                count++;
                                Console.WriteLine("key = {0} size = {1}, Date={2}",
                                entry.Key, entry.Size, entry.LastModified);
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
                        
                        
                        //Console.WriteLine("Next Continuation Token: {0}", response.NextContinuationToken);
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
                }



                Console.WriteLine("Identified {0} billing files", count);
                WriteToBlobStorage(files);
                Console.WriteLine("CopyS2toBlob job has completed sucessfully");
                
            }
        }

        private static void WriteToBlobStorage(Dictionary<string, S3Object> files)
        {
            azureStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureStorageConnectionString")); ;
            azureClient = azureStorageAccount.CreateCloudBlobClient();
            Console.WriteLine(">>> Start copy files to blob storage");
            foreach (var item in files)
            {
                using (client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1))
                {
                    GetObjectRequest request = new GetObjectRequest
                    {
                        BucketName = bucketName,
                        Key = item.Key
                    };
                    
                    using (GetObjectResponse response = client.GetObject(request))
                    {
                        using (Stream responseStream = response.ResponseStream)
                        
                        {
                            
                            var blob = azureClient.GetContainerReference(azureStorageContainerName).GetBlockBlobReference(item.Key);
                            blob.UploadFromStream(responseStream);
                            Console.WriteLine("Copied file {0} to {1}", item.Key, blob.Uri);
                            
                        }

                    }
                }

                Console.WriteLine("File {0} was copied sucessfully", item.Key);
                
            }
            Console.WriteLine(">>> Start copy files to blob storage");
        }
    }
}
