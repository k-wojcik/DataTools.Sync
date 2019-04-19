using System;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Model.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DataTools.Sync.Core
{
    public interface ISyncSetWorker
    {
        Task Sync(SynchronizationSet synchronization);
    }

    public class SyncSetWorker : ISyncSetWorker
    {
        private readonly ITableComparer _tableComparer;
        private readonly ILoadSchema _loadSchema;
        private readonly IDbConnectionFactory _connectionFactory;
        private readonly ILogger<SyncSetWorker> _logger;
        private readonly IServiceProvider _serviceProvider;

        public SyncSetWorker(ITableComparer tableComparer, ILoadSchema loadSchema, IDbConnectionFactory connectionFactory, ILogger<SyncSetWorker> logger, IServiceProvider serviceProvider)
        {

            _tableComparer = tableComparer;
            _loadSchema = loadSchema;
            _connectionFactory = connectionFactory;
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        public async Task Sync(SynchronizationSet synchronization)
        {
            await _loadSchema.Load(synchronization);
            if (await _tableComparer.Compare(synchronization) == false)
            {
                _logger.LogError("Table compare fail for {SyncSetName}", synchronization.Name);
                return;
            }

            foreach (var table in synchronization.Tables)
            {
                _logger.LogInformation("Table {TableName} sync started", table.Name);

                var sourceTable = synchronization.SourceDatabase.Tables.Single(x => x.Name == table.Name);
                var tableSync = _serviceProvider.GetRequiredService<ITableSyncWorker>();
                await tableSync.Sync(synchronization, table, sourceTable);

                _logger.LogInformation("Table {TableName} sync completed", table.Name);
            }
        }
    }
}