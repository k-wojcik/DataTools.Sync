using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Model.Configuration;

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

        public SyncSetWorker(ITableComparer tableComparer, ILoadSchema loadSchema, IDbConnectionFactory connectionFactory)
        {
            _tableComparer = tableComparer;
            _loadSchema = loadSchema;
            _connectionFactory = connectionFactory;
        }

        public async Task Sync(SynchronizationSet synchronization)
        {
            await _loadSchema.Load(synchronization);
            await _tableComparer.Compare(synchronization);

            foreach (var table in synchronization.Tables)
            {
                var sourceTable = synchronization.SourceDatabase.Tables.Single(x => x.Name == table.Name);
                await new TableSyncWorker(_connectionFactory).Sync(synchronization, table, sourceTable);
            }
        }
    }
}