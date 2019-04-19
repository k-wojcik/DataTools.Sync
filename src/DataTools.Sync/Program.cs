using System;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Extensions.Logging;

namespace DataTools.Sync
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("config.json", optional: false, reloadOnChange: true)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddCommandLine(args)
                .Build();

            var container = new ServiceCollection()
                .AddSingleton<IConfigurationRoot>(config)
                .AddSingleton<IConfiguration>(config)
                .AddCore()
                .AddLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.AddConfiguration(config.GetSection("Logging"));
                })
                .BuildServiceProvider();
                ;

            var loggerFactory = container.GetRequiredService<ILoggerFactory>();
            loggerFactory.AddFile(config.GetSection("Logging"));

            var cfg = container.GetRequiredService<IAppConfig>().Get();
            var logger = container.GetRequiredService<ILogger<Program>>();

            foreach (var synchronizationSet in cfg.SynchronizationSets.Where(x => !x.IsDisabled))
            {
                logger.LogInformation("Start {SyncSetName} sync", synchronizationSet.Name);
                await container.GetRequiredService<ISyncSetWorker>().Sync(synchronizationSet);
                logger.LogInformation("End {SyncSetName} sync", synchronizationSet.Name);
            }

            logger.LogInformation("Done!");
            Console.ReadKey();
        }
    }
}
