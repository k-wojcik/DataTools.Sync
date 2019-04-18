using DataTools.Sync.Model.Configuration;
using Microsoft.Extensions.Configuration;

namespace DataTools.Sync.Core
{
    public interface IAppConfig
    {
        Config Get();
    }

    public class AppConfig : IAppConfig
    {
        private readonly IConfiguration _config;

        public AppConfig(IConfiguration config)
        {
            _config = config;
        }

        public Config Get()
        {
            return _config.Get<Config>();
        }
    }
}