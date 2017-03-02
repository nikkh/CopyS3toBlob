

using System;
using System.Collections.Generic;

namespace CopyS3toBlob
{
    public class AwsSettings
    {
        public AwsSettings()
        {
            this.Accounts = new List<AwsAccount>();
            

        }
        public List<AwsAccount> Accounts { get; set; }
        public string Description { get; set; }
    }

    public class AwsAccount
    {
        public AwsAccount()
        {
            this.Buckets = new List<AwsBucket>();
        }
        public string AccountId { get; set; }
        public string AccessKey { get; set; }
        public string SecretAccessKey { get; set; }

        public List<AwsBucket> Buckets { get; set; }

        public override string ToString()
        {
            string s;
            s = string.Format("AccountId={0}, AccessKey=******, SecretAccessKey=******. ", AccountId);
            foreach (var item in Buckets)
            {
                s += string.Format("Bucket={0}, (Region={1}) ", item.Name, item.Region);
                foreach (var pattern in item.Patterns)
                {
                    s += string.Format("Pattern={0} ", pattern);
                }
            }
            return s;
        }

    }

    public class AwsBucket
    {
        public AwsBucket()
        {
            this.Patterns = new List<string>();

        }
        public string Name { get; set; }
        public string Region { get; set; }
        public List<string> Patterns { get; set; }
    }
}
