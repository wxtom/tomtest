using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store.Models;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage.Blob;

namespace USQL
{
    class Program
    {
        private static DataLakeAnalyticsAccountManagementClient _adlaClient;
        private static DataLakeAnalyticsJobManagementClient _adlaJobClient;
        private static DataLakeAnalyticsCatalogManagementClient _adlaCatalogClient;
        private static DataLakeStoreAccountManagementClient _adlsClient;
        private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;

        private static string _adlaAccountName;
        private static string _adlsAccountName;
        private static string _resourceGroupName;
        private static string _location;

        private static void Main(string[] args)
        {
            _adlsAccountName = "xxxxxxxxxx"; // TODO: Replace this value with the name for a created Store account.
            _adlaAccountName = "xxxxxxxxxx"; // TODO: Replace this value with the name for a created Analytics account.
            string localFolderPath = @"C:\tom\"; // TODO: Make sure this exists and contains the U-SQL script.

            // Authenticate the user
            // For more information about applications and instructions on how to get a client ID, see: 
            //   https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/
            var tokenCreds = AuthenticateUser("common", "https://management.core.windows.net/",
                "applicationId", new Uri("http://localhost")); // TODO: Replace bracketed values.

            SetupClients(tokenCreds, "xxxxxxxxxxxxx"); // TODO: Replace bracketed value.

            // Run sample scenarios
            WaitForNewline("Authenticated.", "Creating NEW accounts.");
           //CreateAccounts();
            WaitForNewline("Accounts created.", "Preparing the source data file.");

            // Transfer the source file from a public Azure Blob container to Data Lake Store.
            CloudBlockBlob blob = new CloudBlockBlob(new Uri("https://xxxx.blob.core.windows.net/adls-sample-data/SearchLog.tsv"));
            blob.DownloadToFile(localFolderPath + "SearchLog.tsv", FileMode.Create); // from WASB
            UploadFile(localFolderPath + "SearchLog.tsv", "/mytempdir/SearchLog.tsv"); // to ADLS
            WaitForNewline("Source data file prepared.", "Submitting a job.");

            // Submit the job
            string jobId = SubmitJobByPath(localFolderPath + "SampleUSQLScript.txt", "My First ADLA Job");
            WaitForNewline("Job submitted.", "Waiting for job completion.");

            // Wait for job completion
            WaitForJob(jobId);
            WaitForNewline("Job completed.", "Downloading job output.");

            // Download job output
            DownloadFile("/Output/SearchLog-from-Data-Lake.csv", localFolderPath + "SearchLog-from-Data-Lake.csv");
            WaitForNewline("Job output downloaded.", "Deleting accounts.");

            // Delete accounts
            DeleteAccounts();
            WaitForNewline("Accounts deleted. You can now exit.");
        }

        // Helper function to show status and wait for user input
        public static void WaitForNewline(string reason, string nextAction = "")
        {
            if (!String.IsNullOrWhiteSpace(nextAction))
            {
                Console.WriteLine(reason + "\r\nPress ENTER to continue...");
                Console.ReadLine();
                Console.WriteLine(nextAction);
            }
            else
            {
                Console.WriteLine(reason + "\r\nPress ENTER to continue...");
                Console.ReadLine();
            }
        }

        // Authenticate the user with AAD through an interactive popup.
        // You need to have an application registered with AAD in order to authenticate.
        //   For more information and instructions on how to register your application with AAD, see: 
        //   https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/
        public static TokenCredentials AuthenticateUser(string tenantId, string resource, string appClientId, Uri appRedirectUri, string userId = "")
        {
            var authContext = new AuthenticationContext("https://login.microsoftonline.com/" + tenantId);

            var tokenAuthResult = authContext.AcquireTokenAsync(resource, appClientId, appRedirectUri, new Microsoft.IdentityModel.Clients.ActiveDirectory.PlatformParameters(PromptBehavior.Auto),
                 UserIdentifier.AnyUser).Result;

            return new TokenCredentials(tokenAuthResult.AccessToken);
        }

        // Authenticate the application with AAD through the application's secret key.
        // You need to have an application registered with AAD in order to authenticate.
        //   For more information and instructions on how to register your application with AAD, see: 
        //   https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/
        public static TokenCredentials AuthenticateApplication(string tenantId, string resource, string appClientId, Uri appRedirectUri, ISecureClientSecret clientSecret)
        {
            var authContext = new AuthenticationContext("https://login.microsoftonline.com/" + tenantId);
            var credential = new ClientCredential(appClientId, clientSecret);

            var tokenAuthResult = authContext.AcquireTokenAsync(resource, credential).Result;

            return new TokenCredentials(tokenAuthResult.AccessToken);
        }

        //Set up clients
        public static void SetupClients(TokenCredentials tokenCreds, string subscriptionId)
        {
            _adlaClient = new DataLakeAnalyticsAccountManagementClient(tokenCreds) {SubscriptionId = subscriptionId};

            _adlaJobClient = new DataLakeAnalyticsJobManagementClient(tokenCreds);

            _adlaCatalogClient = new DataLakeAnalyticsCatalogManagementClient(tokenCreds);

            _adlsClient = new DataLakeStoreAccountManagementClient(tokenCreds) {SubscriptionId = subscriptionId};

            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(tokenCreds);
        }


        public static void DeleteAccounts()
        {
            _adlaClient.Account.Delete(_resourceGroupName, _adlaAccountName);

            _adlsClient.Account.Delete(_resourceGroupName, _adlsAccountName);
        }

        // List all ADLA accounts within the subscription
        public static List<DataLakeAnalyticsAccount> ListAdlAnalyticsAccounts()
        {
            var response = _adlaClient.Account.List(_adlaAccountName);
            var accounts = new List<DataLakeAnalyticsAccount>(response);

            while (response.NextPageLink != null)
            {
                response = _adlaClient.Account.ListNext(response.NextPageLink);
                accounts.AddRange(response);
            }

            return accounts;
        }

        // List all ADLS accounts within the subscription
        public static List<DataLakeStoreAccount> ListAdlStoreAccounts()
        {
            var response = _adlsClient.Account.List(_adlsAccountName);
            var accounts = new List<DataLakeStoreAccount>(response);

            while (response.NextPageLink != null)
            {
                response = _adlsClient.Account.ListNext(response.NextPageLink);
                accounts.AddRange(response);
            }

            return accounts;
        }

        // Submit a U-SQL job by providing script contents.
        // Returns the job ID
        public static string SubmitJobByScript(string script, string jobName)
        {
            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(jobName, JobType.USql, properties);

            var jobInfo = _adlaJobClient.Job.Create( _adlaAccountName,jobId, parameters);

            return jobId.ToString();
        }

        // Submit a U-SQL job by providing a path to the script
        public static string SubmitJobByPath(string scriptPath, string jobName)
        {
            var script = File.ReadAllText(scriptPath);
            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(jobName, JobType.USql, properties, priority: 1000, degreeOfParallelism: 1);
            var jobInfo = _adlaJobClient.Job.Create(_adlaAccountName,jobId, parameters);
            return jobId.ToString();
        }

        public static JobResult WaitForJob(string jobId)
        {
            var jobInfo = _adlaJobClient.Job.Get(_adlaAccountName,Guid.Parse(jobId));
            while (jobInfo.State != JobState.Ended)
            {
                jobInfo = _adlaJobClient.Job.Get(_adlaAccountName, Guid.Parse(jobId)); 
            }
            return jobInfo.Result.Value;
        }

        // List jobs
        public static List<JobInformation> ListJobs()
        {
            var response = _adlaJobClient.Job.List(_adlaAccountName);
            var jobs = new List<JobInformation>(response);

            while (response.NextPageLink != null)
            {
                response = _adlaJobClient.Job.ListNext(response.NextPageLink);
                jobs.AddRange(response);
            }

            return jobs;
        }

        // Upload a file
        public static void UploadFile(string srcFilePath, string destFilePath, bool force = true)
        {
            var parameters = new UploadParameters(srcFilePath, destFilePath, _adlsAccountName, isOverwrite: force);
            var frontend = new DataLakeStoreFrontEndAdapter(_adlsAccountName, _adlsFileSystemClient);
            var uploader = new DataLakeStoreUploader(parameters, frontend);
            uploader.Execute();
        }

        // Download file
        public static void DownloadFile(string srcPath, string destPath)
        {
            var stream = _adlsFileSystemClient.FileSystem.Open(srcPath, _adlsAccountName);
            var fileStream = new FileStream(destPath, FileMode.Create);

            stream.CopyTo(fileStream);
            fileStream.Close();
            stream.Close();
        }
    }
}
