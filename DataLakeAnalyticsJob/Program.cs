using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
namespace DataLakeAnalyticsJob
{
    class Program
    {
        private static string _adlaAccountName;
        static void Main(string[] args)
        {
            _adlaAccountName = "tomdatalakeanalytics";
            var applicationId = "1eca141c-d519-40ed-bec6-b237c7fd88b5";
            var secretKey = "H4NsqSHF/LDZFVoddMyqWmLl5CFx/HC2JrTHDotHk3s=";
            var tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
            var subscriptionId = "ed0caab7-c6d4-45e9-9289-c7e5997c9241";
            var adlsAccountName = "brucedatalakestore";
            var creds = ApplicationTokenProvider.LoginSilentAsync(tenantId, applicationId, secretKey).Result;
            var script = File.ReadAllText(@"C:\Tom\CaseDemo\USQL\DataLakeAnalyticsJob\SampleUSQLScript.txt");
            SubmitJob(creds, subscriptionId, script, "tomtest");
        }
        public static void SubmitJob(ServiceClientCredentials tokenCreds, string subscriptionId,string script,string jobName)
        {
            var adlaClient = new DataLakeAnalyticsAccountManagementClient(tokenCreds) {SubscriptionId = subscriptionId};
            var adlaJobClient = new DataLakeAnalyticsJobManagementClient(tokenCreds);
           // var adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(tokenCreds);
            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(jobName, JobType.USql, properties);
            var jobInfo = adlaJobClient.Job.Create(_adlaAccountName,jobId,parameters);
        }
    }
}
