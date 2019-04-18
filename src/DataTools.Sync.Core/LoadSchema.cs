using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Model.Configuration;
using DataTools.Sync.Model.Schema;
using SqlKata.Execution;

namespace DataTools.Sync.Core
{
    public interface ILoadSchema
    {
        Task<bool> Load(SynchronizationSet syncSet);
    }

    public class LoadSchema : ILoadSchema
    {
        private readonly IDbConnectionFactory _connectionFactory;
        private SynchronizationSet _syncSet;
        private QueryFactory _sourceQuery;
        private QueryFactory _destinationQuery;

        public LoadSchema(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public async Task<bool> Load(SynchronizationSet syncSet)
        {
            _syncSet = syncSet;
            _sourceQuery = _connectionFactory.GetSource(syncSet.Name);
            _destinationQuery = _connectionFactory.GetDestination(syncSet.Name);

            List<TableSchema> sourceTables = (await GetTables(_sourceQuery)).ToList();
            List<TableSchema> destinationTables = (await GetTables(_destinationQuery)).ToList();

            foreach (var table in sourceTables)
            {
                table.Columns = (await GetTableColumns(_sourceQuery, table.ObjectId)).ToList();
            }

            foreach (var table in destinationTables)
            {
                table.Columns = (await GetTableColumns(_destinationQuery, table.ObjectId)).ToList();
            }

            syncSet.SourceDatabase = new DatabaseSchema()
            {
                Tables = sourceTables
            };
            syncSet.DestinationDatabase = new DatabaseSchema()
            {
                Tables = destinationTables
            };
            return true;
        }

        private Task<IEnumerable<TableSchema>> GetTables(QueryFactory queryFactory)
        {
            return queryFactory.Query("sys.objects")
                .Where("type_desc", "=", "USER_TABLE")
                .Select("object_id AS ObjectId", "name AS Name")
                .GetAsync<TableSchema>();
        }

        private Task<IEnumerable<ColumnSchema>> GetTableColumns(QueryFactory queryFactory, int objectId)
        {
            var query = _sourceQuery.Query("sys.columns AS c")
                .Join("sys.types as t", "t.user_type_id", "c.user_type_id")
                .Join("sys.objects as o", "o.object_id", "c.object_id")
                .Where("c.object_id", "=", objectId)
                .Select(
                    "c.column_id AS ColumnId",
                    "c.name AS Name",
                    "c.is_identity AS IsIdentity",
                    "c.is_nullable AS IsNullable",
                    "c.is_computed AS IsComputed",
                    "c.max_length AS MaxLength",
                    "c.precision AS Precision",
                    "c.scale AS Scale",
                    "t.name AS Type"
                )
                .SelectRaw(@"
                    ISNULL(
                        (SELECT 1
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS co
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K
                            ON co.TABLE_NAME = K.TABLE_NAME
                            AND co.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG
                            AND co.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA
                            AND co.CONSTRAINT_NAME = K.CONSTRAINT_NAME
                        WHERE co.CONSTRAINT_TYPE = 'PRIMARY KEY' AND co.TABLE_NAME = o.name AND K.COLUMN_NAME = c.name
                ), 0)  AS IsPrimaryKey");
            
            return query.GetAsync<ColumnSchema>();
        }
    }
}