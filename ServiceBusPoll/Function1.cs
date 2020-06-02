using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Configuration;
using System.Net;
using Microsoft.Azure.Amqp.Framing;

namespace ServiceBusPoll02
{
    public static class Function1
    {
        private static Settings settings;
        [FunctionName("PlayCountPoll")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {
            GetSettings(context, log);
            string[] playTypes = { "offense_sideline", "offense_endzone", "defense_sideline", "defense_endzone", "specialteams" };
            var managementClient = new ManagementClient(settings.ServiceBusConnectionString);

            long count = 0;

            foreach (string plays in playTypes)
            {
                var queue = await managementClient.GetQueueRuntimeInfoAsync(plays);
                var messageCount = queue.MessageCount;

                count += messageCount;
            }
            await managementClient.CloseAsync();

            if (count > 0)
            {
                req.HttpContext.Response.Headers.Add("location", settings.FunctionURL);
                req.HttpContext.Response.Headers.Add("retry-after", "30");
                return new StatusCodeResult((int)HttpStatusCode.Accepted);
            }

            return new OkObjectResult("Completed");
        }

        static void GetSettings(ExecutionContext context, ILogger log)
        {
            try
            {
                var config = new ConfigurationBuilder()
                    .SetBasePath(context.FunctionAppDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                settings = new Settings();
                settings.ServiceBusConnectionString = config["ServiceBusConnectionString"];
                settings.FunctionURL = config["FunctionURL"];
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
            }
        }
    }
    class Settings
    {
        public string ServiceBusConnectionString { get; set; }
        public string FunctionURL { get; set; }
    }
}
