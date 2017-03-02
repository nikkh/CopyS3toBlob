using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CopyS3toBlob
{
    public class BucketBlobPackage
    {
        public BucketBlobPackage()
        {
            this.FileInfo = new Dictionary<string, S3Object>();
        }
        public Dictionary<string, S3Object> FileInfo { get; set; }
        public AwsBucket BucketInfo { get; set; }
        public string AccountId { get; set; }
        public string AccessKey { get; set; }
        public string SecretAccessKey { get; set; }
    }
}
