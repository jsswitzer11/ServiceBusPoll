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
using System.Collections.Generic;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus;
using System.Text;

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

            string GameKey = req.Query["GameKey"];

            long count = 0;

            List<string> gameKeys = new List<string>();
            foreach (string plays in playTypes)
            {
                IMessageReceiver messageReceiver = new MessageReceiver(settings.ServiceBusConnectionString, plays, ReceiveMode.PeekLock);

                var queue = await managementClient.GetQueueRuntimeInfoAsync(plays);
                var messageCount = queue.MessageCount;

                count += messageCount;

                // Retrieve the exact number of messages for each queue
                var messages = await messageReceiver.PeekAsync((int)count);

                foreach (Message ms in messages)
                {
                    
                    var newGameMessage = JsonConvert.DeserializeObject<messageBody>(Encoding.UTF8.GetString(ms.Body));

                    gameKeys.Add(newGameMessage.gamekey);                    
                }
            }

            await managementClient.CloseAsync();

            log.LogInformation($"Game Key is {GameKey}");

            if (gameKeys.Contains(GameKey))
            {
                req.HttpContext.Response.Headers.Add("location", settings.FunctionURL+"&GameKey="+GameKey);
                req.HttpContext.Response.Headers.Add("retry-after", "30");
                log.LogInformation($"Messages remaining for game {GameKey}: {count}");
                return new StatusCodeResult((int)HttpStatusCode.Accepted);
            }

            log.LogInformation($"Message count for {GameKey}: {count}");
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
    class messageBody
    {
        public string gamekey { get; set; }
    }
}
