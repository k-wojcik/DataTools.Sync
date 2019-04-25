using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
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
        private readonly IDbQueryFactory _queryFactory;
        private readonly ILogger<TableSyncWorker> _logger;
        private SynchronizationSet _syncSet;
        private QueryFactory _sourceQueryFactory;
        private QueryFactory _destinationQueryFactory;
        private TableSchema _tableSchema;
        private Table _table;
        private readonly BufferedQueue<dynamic> _sourceBuffer;
        private readonly BufferedQueue<dynamic> _destinationBuffer;
        private IList<ColumnSchema> _primaryKeys;
        private IList<ColumnSchema> _identityColumns;
        private Query _sourceQuery;
        private Query _destinationQuery;
        private IList<IDictionary<string, object>> _insertBuffer = new List<IDictionary<string, object>>();
        private IList<IDictionary<string, object>> _updateBuffer = new List<IDictionary<string, object>>();
        private ManualResetEventSlim _insertWorkerEvent = new ManualResetEventSlim();
        private ReaderWriterLockSlim _insertDestinationLock = new ReaderWriterLockSlim();
        private ManualResetEventSlim _updateWorkerEvent = new ManualResetEventSlim();
        private ReaderWriterLockSlim _updateDestinationLock = new ReaderWriterLockSlim();
        private const int SqlServerBatchLimit = 2000;
        private CancellationTokenSource _cancellationToken = new CancellationTokenSource();

        public TableSyncWorker(IDbQueryFactory queryFactory, ILogger<TableSyncWorker> logger)
        {
            _queryFactory = queryFactory;
            _logger = logger;
            _sourceBuffer = new BufferedQueue<dynamic>(LoadSource, 1000);
            _destinationBuffer = new BufferedQueue<dynamic>(LoadDestination, 1000);
        }

        private BufferRange<dynamic> LoadSource(BufferRange<dynamic> arg)
        {
            int retryCount = 5;
            do
            {
                try
                {
                    arg.Result = _sourceQuery.Clone().Offset(arg.Offset).Take(arg.Size).Get<dynamic>().ToList();
                    retryCount = 0;
                }
                catch (SqlException e)
                {
                    _logger.LogError(e, "{TableName} - timeout: {Offset} size: {Size}", _table.Name, arg.Offset, arg.Size);
                }
            } while (retryCount-- > 0);

            _logger.LogDebug("{TableName} - load source offset: {Offset} size: {Size} count: {Count}", _table.Name, arg.Offset, arg.Size, arg.Result.Count);
            return arg;
        }

        private BufferRange<dynamic> LoadDestination(BufferRange<dynamic> arg)
        {
            int retryCount = 5;
            do
            {
                try
                {
                    arg.Result = _destinationQuery.Clone().Offset(arg.Offset).Take(arg.Size).Get<dynamic>().ToList();
                    retryCount = 0;
                }
                catch (SqlException e)
                {
                    _logger.LogError(e, "{TableName} - timeout: {Offset} size: {Size}", _table.Name, arg.Offset, arg.Size);
                }
            } while (retryCount-- > 0);
          

            _logger.LogDebug("{TableName} - load destination offset: {Offset} size: {Size} count: {Count}", _table.Name, arg.Offset, arg.Size, arg.Result.Count);
            return arg;
        }

        public async Task<bool> Sync(SynchronizationSet syncSet, Table table, TableSchema tableSchema)
        {
            _logger.LogInformation("{TableName} - sync started", table.Name);

            _syncSet = syncSet;
            _table = table;
            _tableSchema = tableSchema;
            _sourceQueryFactory = _queryFactory.GetSource(syncSet.Name);
            _destinationQueryFactory = _queryFactory.GetDestination(syncSet.Name);
            _primaryKeys = _tableSchema.Columns.Where(x => x.IsPrimaryKey).ToList();
            _identityColumns = _tableSchema.Columns.Where(x => x.IsIdentity).ToList();
            _sourceQuery = BuildSourceQuery(BuildMainQuery(_sourceQueryFactory));
            _destinationQuery = BuildMainQuery(_destinationQueryFactory);

            ThreadPool.QueueUserWorkItem(InsertWorker);
            ThreadPool.QueueUserWorkItem(UpdateWorker);

            Merge();
            FinalizeInsertDestination();
            FinalizeUpdateDestination();

            _cancellationToken.Cancel();
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
                    UpdateDestination((IDictionary<string, object>)currentSourceRow);

                    _sourceBuffer.TryDequeue(out currentSourceRow);
                    _destinationBuffer.TryDequeue(out currentDestinationRow);
                }
                else if (joinResult == MergeJoinResult.DestinationNotExists)
                {
                    InsertDestination((IDictionary<string, object>)currentSourceRow);

                    _sourceBuffer.TryDequeue(out currentSourceRow);
                }
                else if (joinResult == MergeJoinResult.SourceNotExists)
                {
                    if (_table.AllowDeleteDestination == true)
                    {
                        DeleteDestination((IDictionary<string, object>)currentDestinationRow);
                    }

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
                int result = 0;
                if (column.Type == "varchar" || column.Type == "nvarchar" || column.Type == "char" || column.Type == "nchar" || column.Type == "text" || column.Type == "ntext")
                {
                    result = string.Compare(source[column.Name]?.ToString(), destination[column.Name]?.ToString(), StringComparison.OrdinalIgnoreCase);
                }
                else if (column.Type == "tinyint" || column.Type == "smallint" || column.Type == "int" || column.Type == "bigint")
                {
                    result = Comparer.DefaultInvariant.Compare(source[column.Name], destination[column.Name]);
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

        private void InsertWorker(object state)
        {
            int bufferMaxSize = 5000;
            
            while (_cancellationToken.IsCancellationRequested != true)
            {
                WaitHandle.WaitAny(new[] { _cancellationToken.Token.WaitHandle, _insertWorkerEvent.WaitHandle });

                if (_insertBuffer.Count > bufferMaxSize)
                {
                    try
                    {
                        _insertDestinationLock.EnterWriteLock();

                        var row = _insertBuffer.First();
                        int maxInnerBatchSize = SqlServerBatchLimit / row.Keys.Count;

                        Parallel.ForEach(_insertBuffer.Batch(maxInnerBatchSize), new ParallelOptions() {MaxDegreeOfParallelism = 3}, batch =>
                        {
                            InsertDestination(batch.ToArray());
                        });
                        _insertBuffer = new List<IDictionary<string, object>>();

                    }
                    finally
                    {
                        _insertDestinationLock.ExitWriteLock();
                    }
                }
            }
        }

        private void InsertDestination(IDictionary<string, object> row)
        {
            if (_syncSet.IsDryRun)
            {
                return;
            }

            try
            {
                _insertDestinationLock.EnterReadLock();
                _insertBuffer.Add(row);
                _insertWorkerEvent.Set();
            }
            finally
            {
                _insertDestinationLock.ExitReadLock();
            }
        }

        private void FinalizeInsertDestination()
        {
            if (_syncSet.IsDryRun)
            {
                return;
            }

            if (_insertBuffer.Count > 0)
            {
                var row = _insertBuffer.First();
                int maxInnerBatchSize = SqlServerBatchLimit / row.Keys.Count;
                foreach (var batch in _insertBuffer.Batch(maxInnerBatchSize))
                {
                    InsertDestination(batch.ToArray());
                }
                _insertBuffer = new List<IDictionary<string, object>>();
            }
        }

        private void UpdateWorker(object state)
        {
            int maxSizeBuffer = 5000;
            int maxBatchSize = 1000;

            while (_cancellationToken.IsCancellationRequested != true)
            {
                WaitHandle.WaitAny(new[] { _cancellationToken.Token.WaitHandle, _updateWorkerEvent.WaitHandle });

                if (_updateBuffer.Count > maxSizeBuffer)
                {
                    try
                    {
                        _updateDestinationLock.EnterWriteLock();
                        Parallel.ForEach(_updateBuffer.Batch(maxBatchSize), new ParallelOptions() { MaxDegreeOfParallelism = 3 }, batch => { UpdateDestination(batch.ToArray()); });
                        _updateBuffer = new List<IDictionary<string, object>>();

                    }
                    finally
                    {
                        _updateDestinationLock.ExitWriteLock();
                    }
                }
            }
        }

        private void UpdateDestination(IDictionary<string, object> row)
        {
            if (_syncSet.IsDryRun || _table.OnlyInsert == true)
            {
                return;
            }

            try
            {
                _updateDestinationLock.EnterReadLock();
                _updateBuffer.Add(row);
                _updateWorkerEvent.Set();
            }
            finally
            {
                _updateDestinationLock.ExitReadLock();
            }
        }

        private void FinalizeUpdateDestination()
        {
            if (_syncSet.IsDryRun || _table.OnlyInsert == true)
            {
                return;
            }

            if (_updateBuffer.Count > 0)
            {
                UpdateDestination(_updateBuffer.ToArray());
                _updateBuffer = new List<IDictionary<string, object>>();
            }
        }

        private void InsertDestination(IDictionary<string, object>[] rows)
        {
            if (_syncSet.IsDryRun)
            {
                return;
            }

            var queryFactory = _queryFactory.GetDestination(_syncSet.Name);
            SetIdentityInsertOn(queryFactory);

            var firstRow = rows.First();
            var insertQuery = queryFactory.Query(_tableSchema.Name).AsInsert(firstRow.Select(x=>x.Key), rows.Select(x => x.Values));

            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Insert {TableName} {Sql}", _table.Name, queryFactory.Compiler.Compile(insertQuery).ToString());
            }

            int retryCount = 5;
            do
            {
                try
                {
                    insertQuery.Get();
                    retryCount = 0;
                }
                catch (SqlException e)
                {
                    _logger.LogError(e, "{TableName} - insert error", _table.Name);
                }
            } while (retryCount-- > 0);

            SetIdentityInsertOff(queryFactory);
        }

        private void UpdateDestination(IDictionary<string, object>[] rows)
        {
            if (_syncSet.IsDryRun || _table.OnlyInsert == true)
            {
                return;
            }

            var queryFactory = _queryFactory.GetDestination(_syncSet.Name);
            foreach (var row in rows)
            {
                var updateQuery = queryFactory.Query(_tableSchema.Name)
                    .Where(row.Where(x =>
                            _primaryKeys.Any(pk => pk.Name == x.Key)).ToDictionary(x => x.Key, x => x.Value)
                    )
                    .AsUpdate(row.Where(x => _identityColumns.All(y => y.Name != x.Key)).ToDictionary(x => x.Key, x => x.Value));

                if (_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace("update {TableName} {Sql}", _table.Name, queryFactory.Compiler.Compile(updateQuery).ToString());
                }

                int retryCount = 5;
                do
                {
                    try
                    {
                        updateQuery.Get();
                        retryCount = 0;
                    }
                    catch (SqlException e)
                    {
                        _logger.LogError(e, "{TableName} - update error", _table.Name);
                    }
                } while (retryCount-- > 0);
            }
        }

        private void DeleteDestination(IDictionary<string, object> row)
        {
            if (_syncSet.IsDryRun || _table.OnlyInsert == true)
            {
                return;
            }

            var queryFactory = _queryFactory.GetDestination(_syncSet.Name);
            var deleteQuery = queryFactory.Query(_tableSchema.Name)
                .Where(row.Where(x =>
                        _primaryKeys.Any(pk => pk.Name == x.Key)).ToDictionary(x => x.Key, x => x.Value)
                )
                .AsDelete();

            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("delete {TableName} {Sql}", _table.Name, queryFactory.Compiler.Compile(deleteQuery).ToString());
            }

            deleteQuery.Get();
        }

        private Query BuildMainQuery(QueryFactory queryFactory)
        {
            var query = queryFactory.Query(_tableSchema.Name);

            foreach (var column in _tableSchema.Columns)
            {
                query.Select($"{_table.Alias ?? _tableSchema.Name}.{column.Name}");
            }

            if (!string.IsNullOrWhiteSpace(_table.From))
            {
                query.FromRaw(ReplaceVariables(_table.From));
            }

            if (!string.IsNullOrWhiteSpace(_table.Filter))
            {
                query.WhereRaw(ReplaceVariables(_table.Filter));
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
                    query.OrderBy($"{_table.Alias ?? _tableSchema.Name}.{primaryKey.Name}");
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

        private void SetIdentityInsertOn(QueryFactory queryFactory)
        {
            if (_identityColumns.Any())
            {
              //  _logger.LogDebug("{TableName} - set identity insert on", _table.Name);
                queryFactory.Statement($"SET IDENTITY_INSERT {_table.Name} ON");
            }
        }

        private void SetIdentityInsertOff(QueryFactory queryFactory)
        {
            if (_identityColumns.Any())
            {
            //    _logger.LogDebug("{TableName} - set identity insert off", _table.Name);
                queryFactory.Statement($"SET IDENTITY_INSERT {_table.Name} OFF");
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