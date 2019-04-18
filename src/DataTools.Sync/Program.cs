using System;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataTools.Sync
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("config.json", optional: false, reloadOnChange: true)
                .AddCommandLine(args)
                .Build();

            var container = new ServiceCollection()
                    .AddSingleton<IConfigurationRoot>(config)
                    .AddSingleton<IConfiguration>(config)
                    .AddCore()
                    .BuildServiceProvider()
                ;

            var cfg = container.GetRequiredService<IAppConfig>().Get();

            foreach (var synchronizationSet in cfg.SynchronizationSets.Where(x=>!x.IsDisabled))
            {
                await container.GetRequiredService<ISyncSetWorker>().Sync(synchronizationSet);
            }

            Console.WriteLine("Done!");
            Console.ReadKey();
        }
    }
}
