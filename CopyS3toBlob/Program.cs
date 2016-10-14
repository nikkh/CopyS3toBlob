using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Amazon.S3;
using Amazon.S3.Model;
using System.IO;

namespace CopyS3toBlob
{
    // To learn more about Microsoft Azure WebJobs SDK, please see http://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        static IAmazonS3 client;
        static string bucketName = "nh-usage-1";


        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        static void Main()
        {
            Console.WriteLine("CopyS3toBlob job has started...");
            using (client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1))
            {
                Console.WriteLine("Listing objects stored in bucket: "+bucketName);


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
                            if (entry.Key.ToLower().Contains("zip"))
                            {
                                Console.WriteLine("key = {0} size = {1}",
                                entry.Key, entry.Size);
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


             
               
                Console.WriteLine("CopyS2toBlob job has completed sucessfully");
                Console.ReadKey();
            }
        }
    }
}
