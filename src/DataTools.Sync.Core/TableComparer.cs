using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Model.Configuration;
using DataTools.Sync.Model.Schema;
using Microsoft.Extensions.Logging;
using SqlKata.Execution;

namespace DataTools.Sync.Core
{
    public interface ITableComparer
    {
        Task<bool> Compare(SynchronizationSet syncSet);
    }

    public class TableComparer : ITableComparer
    {
        private readonly IDbConnectionFactory _connectionFactory;
        private readonly ILogger<TableComparer> _logger;
        private SynchronizationSet _syncSet;
        private QueryFactory _sourceQuery;
        private QueryFactory _destinationQuery;

        public TableComparer(IDbConnectionFactory connectionFactory, ILogger<TableComparer> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        public async Task<bool> Compare(SynchronizationSet syncSet)
        {
            _syncSet = syncSet;
            _sourceQuery = _connectionFactory.GetSource(syncSet.Name);
            _destinationQuery = _connectionFactory.GetDestination(syncSet.Name);
            
            return Compare(syncSet.SourceDatabase.Tables, syncSet.DestinationDatabase.Tables);
        }

        private bool Compare(List<TableSchema> sourceTables, List<TableSchema> destinationTables)
        {
            bool isValid = true;
            foreach (var table in _syncSet.Tables)
            {
                _logger.LogInformation("Compare table {TableName}", table.Name);

                var sourceTable = sourceTables.FirstOrDefault(x => x.Name == table.Name);
                var destinationTable = destinationTables.FirstOrDefault(x => x.Name == table.Name);

                if (sourceTable == null || destinationTable == null)
                {
                    _logger.LogError("{TableSource} table {TableName} not exist", sourceTable == null ? "Source" : "Destination", table.Name);
                    isValid = false;
                    continue;
                }

                if (!Compare(table, sourceTable.Columns, destinationTable.Columns))
                {
                    _logger.LogError("Table compare for {TableName} failed", table.Name);
                    isValid = false;
                }
                else
                {
                    _logger.LogInformation("Compare table {TableName} - valid", table.Name);
                }
            }

            return isValid;
        }

        private bool Compare(Table table, List<ColumnSchema> sourceColumns, List<ColumnSchema> destinationColumns)
        {
            bool isValid = true;
            foreach (var sourceColumn in sourceColumns)
            {
                var destinationColumn = destinationColumns.FirstOrDefault(x => x.Name == sourceColumn.Name);
                if (destinationColumn == null)
                {
                    continue;
                }

                if (sourceColumn.IsIdentity != destinationColumn.IsIdentity)
                {
                    _logger.LogError("Source/Destination {TableName}.{ColumnName} identity column mismatch", table.Name, sourceColumn.Name);
                    isValid = false;
                }

                if (sourceColumn.IsComputed)
                {
                    _logger.LogError("Source column {TableName}.{ColumnName} is computed", table.Name, sourceColumn.Name);
                    isValid = false;
                }

                if (destinationColumn.IsComputed)
                {
                    _logger.LogError("Destination column {TableName}.{ColumnName} is computed", table.Name, destinationColumn.Name);
                    isValid = false;
                }

                if (sourceColumn.IsNullable != destinationColumn.IsNullable && destinationColumn.IsNullable == false)
                {
                    _logger.LogError("Destination column {TableName}.{ColumnName} is not nullable when source column is nullable!", table.Name, destinationColumn.Name);
                    isValid = false;
                }

                if (sourceColumn.Type != destinationColumn.Type)
                {
                    _logger.LogError("Destination/source column {TableName}.{ColumnName} type mismatch (source: {SourceColumnType} destination: {DestinationColumnType})!", table.Name, destinationColumn.Name, sourceColumn.Type, destinationColumn.Type);
                    isValid = false;
                }

                if (sourceColumn.MaxLength > destinationColumn.MaxLength && destinationColumn.MaxLength != -1)
                {
                    _logger.LogError("Source column {TableName}.{ColumnName} has a longer length (source: {SourceColumnMaxLength} destination: {DestinationColumnMaxLength})!", table.Name, destinationColumn.Name, sourceColumn.MaxLength, destinationColumn.MaxLength);
                    isValid = false;
                }

                if (sourceColumn.Precision != destinationColumn.Precision)
                {

                }

                if (sourceColumn.Scale != destinationColumn.Scale)
                {

                }
            }

            return isValid;
        }

    }
}