using Microsoft.Extensions.DependencyInjection;

namespace DataTools.Sync.Core
{
    public static class SyncCoreExtension
    {
        public static IServiceCollection AddCore(this IServiceCollection services)
        {
            services.AddSingleton<IAppConfig, AppConfig>();
            services.AddSingleton<IDbConnectionFactory, DbConnectionFactory>();
            services.AddTransient<ILoadSchema, LoadSchema>();
            services.AddTransient<ITableComparer, TableComparer>();
            services.AddTransient<ISyncSetWorker, SyncSetWorker>();
            services.AddTransient<ITableSyncWorker, TableSyncWorker>();
            return services;
        }
    }
}