using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Model.Configuration;
using DataTools.Sync.Model.Schema;
using Microsoft.Extensions.Logging;
using SqlKata;
using SqlKata.Execution;

namespace DataTools.Sync.Core
{
    public interface ITableSyncWorker
    {
        Task<bool> Sync(SynchronizationSet syncSet, Table table, TableSchema tableSchema);
    }

    public class TableSyncWorker : ITableSyncWorker
    {
        private readonly IDbConnectionFactory _connectionFactory;
        private readonly ILogger<TableSyncWorker> _logger;
        private SynchronizationSet _syncSet;
        private QueryFactory _sourceQueryFactory;
        private QueryFactory _destinationQueryFactory;
        private TableSchema _tableSchema;
        private Table _table;
        private BufferedQueue<dynamic> _sourceBuffer;
        private BufferedQueue<dynamic> _destinationBuffer;
        private IList<ColumnSchema> _primaryKeys;
        private IList<ColumnSchema> _identityColumns;
        private Query _sourceQuery;
        private Query _destinationQuery;

        public TableSyncWorker(IDbConnectionFactory connectionFactory, ILogger<TableSyncWorker> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
            _sourceBuffer = new BufferedQueue<dynamic>(LoadSource, 1000);
            _destinationBuffer = new BufferedQueue<dynamic>(LoadDestination, 1000);
        }

        private BufferRange<dynamic> LoadSource(BufferRange<dynamic> arg)
        {
            arg.Result = _sourceQuery.Clone().Offset(arg.Offset).Take(arg.Size).Get<dynamic>().ToList();
            _logger.LogDebug("{TableName} - load source offset: {Offset} size: {Size} count: {Count}", _table.Name, arg.Offset, arg.Size, arg.Result.Count);
            return arg;
        }

        private BufferRange<dynamic> LoadDestination(BufferRange<dynamic> arg)
        {
            arg.Result = _destinationQuery.Clone().Offset(arg.Offset).Take(arg.Size).Get<dynamic>().ToList();
            _logger.LogDebug("{TableName} - load destination offset: {Offset} size: {Size} count: {Count}", _table.Name, arg.Offset, arg.Size, arg.Result.Count);
            return arg;
        }

        public async Task<bool> Sync(SynchronizationSet syncSet, Table table, TableSchema tableSchema)
        {
            _logger.LogInformation("{TableName} - sync started", table.Name);

            _syncSet = syncSet;
            _table = table;
            _tableSchema = tableSchema;
            _sourceQueryFactory = _connectionFactory.GetSource(syncSet.Name);
            _destinationQueryFactory = _connectionFactory.GetDestination(syncSet.Name);
            _primaryKeys = _tableSchema.Columns.Where(x => x.IsPrimaryKey).ToList();
            _identityColumns = _tableSchema.Columns.Where(x => x.IsIdentity).ToList();
            _sourceQuery = BuildSourceQuery(BuildMainQuery(_sourceQueryFactory));
            _destinationQuery = BuildMainQuery(_destinationQueryFactory);

            SetIdentityInsertOn();
            Merge();
            SetIdentityInsertOff();

            _logger.LogInformation("{TableName} - sync completed", table.Name);
            return true;
        }

        private void Merge()
        {
            _sourceBuffer.TryDequeue(out dynamic currentSourceRow);
            _destinationBuffer.TryDequeue(out dynamic currentDestinationRow);

            while (currentSourceRow != null || currentDestinationRow != null)
            {
                var joinResult = Join(currentSourceRow, currentDestinationRow);
                if (_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace("Compare {TableName} {SourceRaw} {DestinationRaw} = {JoinResult}", _table.Name, (IDictionary<string, object>) currentSourceRow, (IDictionary<string, object>) currentDestinationRow, (object) joinResult);
                }

                if (joinResult == MergeJoinResult.Equal)
                {
                    UpdateDestination(currentSourceRow);

                    _sourceBuffer.TryDequeue(out currentSourceRow);
                    _destinationBuffer.TryDequeue(out currentDestinationRow);
                }
                else if (joinResult == MergeJoinResult.DestinationNotExists)
                {
                    InsertDestination(currentSourceRow);

                    _sourceBuffer.TryDequeue(out currentSourceRow);
                }
                else if (joinResult == MergeJoinResult.SourceNotExists)
                {
                    _destinationBuffer.TryDequeue(out currentDestinationRow);
                }
            }
        }

        private MergeJoinResult Join(IDictionary<string, object> source, IDictionary<string, object> destination)
        {
            if (destination == null)
            {
                return MergeJoinResult.DestinationNotExists;
            }

            if (source == null)
            {
                return MergeJoinResult.SourceNotExists;
            }

            for (int i = 0; i < _primaryKeys.Count; i++)
            {
                ColumnSchema column = _primaryKeys[i];
                int result;
                if (column.Type == "varchar" || column.Type == "nvarchar" || column.Type == "char" || column.Type == "nchar" || column.Type == "text" || column.Type == "ntext")
                {
                    result = string.Compare(source[column.Name].ToString(), destination[column.Name].ToString(), StringComparison.OrdinalIgnoreCase);
                    if (result < 0)
                    {
                        return MergeJoinResult.DestinationNotExists;
                    }
                    if (result > 0)
                    {
                        return MergeJoinResult.SourceNotExists;
                    }
                }
                else if (column.Type == "tinyint" || column.Type == "smallint" || column.Type == "int" || column.Type == "bigint")
                {
                    result = Convert.ToInt64((source[column.Name])).CompareTo(Convert.ToInt64(destination[column.Name]));
                }
                else
                {
                    throw new NotSupportedException($"Not supported column type {column.Type}");
                }

                if (result < 0)
                {
                    return MergeJoinResult.DestinationNotExists;
                }
                if (result > 0)
                {
                    return MergeJoinResult.SourceNotExists;
                }
            }

            return MergeJoinResult.Equal;
        }

        private void InsertDestination(IDictionary<string, object> row)
        {
            var insertQuery = _destinationQueryFactory.Query(_tableSchema.Name).AsInsert(row.ToDictionary(x => x.Key, x => x.Value));

            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Insert {TableName} {Sql}", _table.Name, _destinationQueryFactory.Compiler.Compile(insertQuery).ToString());
            }
           
            insertQuery.Get();
        }

        private void UpdateDestination(IDictionary<string, object> row)
        {
            var updateQuery = _destinationQueryFactory.Query(_tableSchema.Name)
                .Where(row.Where(x =>
                        _primaryKeys.Any(pk => pk.Name == x.Key)).ToDictionary(x => x.Key, x => x.Value)
                )
                .AsUpdate(row.Where(x => _identityColumns.All(y => y.Name != x.Key)).ToDictionary(x => x.Key, x => x.Value));

            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("update {TableName} {Sql}", _table.Name, _destinationQueryFactory.Compiler.Compile(updateQuery).ToString());
            }

            updateQuery.Get();
        }

        private Query BuildMainQuery(QueryFactory queryFactory)
        {
            var query = queryFactory.Query(_tableSchema.Name);

            foreach (var column in _tableSchema.Columns)
            {
                query.Select($"{_tableSchema.Name}.{column.Name}");
            }

            if (_table.SortColumns != null && _table.SortColumns.Length > 0)
            {
                foreach (var column in _table.SortColumns)
                {
                    query.OrderBy($"{column}");
                }
            }
            else if (_primaryKeys.Count > 0)
            {
                foreach (var primaryKey in _primaryKeys)
                {
                    query.OrderBy($"{_tableSchema.Name}.{primaryKey.Name}");
                }
            }
            else
            {
                throw new ArgumentException($"Sort order is required {_tableSchema.Name}");
            }

            return query;
        }

        public Query BuildSourceQuery(Query query)
        {
            if (!string.IsNullOrWhiteSpace(_table.SourceFrom))
            {
                query.FromRaw(ReplaceVariables(_table.SourceFrom));
            }

            if (!string.IsNullOrWhiteSpace(_table.SourceFilter))
            {
                query.WhereRaw(ReplaceVariables(_table.SourceFilter));
            }

            return query;
        }

        private string ReplaceVariables(string input)
        {
            foreach (var variable in _syncSet.Variables)
            {
                input = input.Replace($"@{variable.Key}", variable.Value);
            }
            return input;
        }

        private void SetIdentityInsertOn()
        {
            if (_identityColumns.Any())
            {
                _logger.LogDebug("{TableName} - set identity insert on", _table.Name);
                _destinationQueryFactory.Statement($"SET IDENTITY_INSERT {_table.Name} ON");
            }
        }

        private void SetIdentityInsertOff()
        {
            if (_identityColumns.Any())
            {
                _logger.LogDebug("{TableName} - set identity insert off", _table.Name);
                _destinationQueryFactory.Statement($"SET IDENTITY_INSERT {_table.Name} OFF");
            }
        }
    }

    enum MergeJoinResult
    {
        Equal = 1,
        DestinationNotExists = 2,
        SourceNotExists = 3,
    }
}