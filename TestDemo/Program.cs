using System;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;

namespace TestDemo
{
    class Program
    {
        static void Main(string[] args)
        {

        }
        public static TokenCredentials AuthenticateUser(string tenantId, string resource, string appClientId, Uri appRedirectUri, string userId = "")
        {
            var authContext = new AuthenticationContext("https://login.microsoftonline.com/" + tenantId);

            var tokenAuthResult = authContext.AcquireTokenAsync(resource, appClientId, appRedirectUri, new Microsoft.IdentityModel.Clients.ActiveDirectory.PlatformParameters(PromptBehavior.Auto),
                 UserIdentifier.AnyUser).Result;

            return new TokenCredentials(tokenAuthResult.AccessToken);
        }
    }
}
