using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataTools.Sync.Model.Configuration;
using DataTools.Sync.Model.Schema;
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
        private SynchronizationSet _syncSet;
        private QueryFactory _sourceQuery;
        private QueryFactory _destinationQuery;

        public TableComparer(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
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
            foreach (var table in _syncSet.Tables)
            {
                var sourceTable = sourceTables.FirstOrDefault(x => x.Name == table.Name);
                var destinationTable = destinationTables.FirstOrDefault(x => x.Name == table.Name);

                if (sourceTable == null || destinationTable == null)
                {
                    return false;
                }

                if (!Compare(table, sourceTable.Columns, destinationTable.Columns))
                {
                    return false;
                }
            }

            return true;
        }

        private bool Compare(Table table, List<ColumnSchema> sourceColumns, List<ColumnSchema> destinationColumns)
        {
            foreach (var sourceColumn in sourceColumns)
            {
                var destinationColumn = destinationColumns.FirstOrDefault(x => x.Name == sourceColumn.Name);
                if (destinationColumn == null)
                {
                    continue;
                }

                if (sourceColumn.IsIdentity != destinationColumn.IsIdentity)
                {
                    throw new ArgumentException($"Source/Destination {table.Name}.{sourceColumn.Name} identity column mismatch");
                }

                if (sourceColumn.IsComputed)
                {
                    throw new ArgumentException($"Source column {table.Name}.{sourceColumn.Name} is computed");
                }

                if (destinationColumn.IsComputed)
                {
                    throw new ArgumentException($"Destination column {table.Name}.{destinationColumn.Name} is computed");
                }

                if (sourceColumn.IsNullable != destinationColumn.IsNullable && destinationColumn.IsNullable == false)
                {
                    throw new ArgumentException($"Destination column {table.Name}.{destinationColumn.Name} is not nullable when source column is nullable!");
                }

                if (sourceColumn.Type != destinationColumn.Type)
                {
                    throw new ArgumentException($"Destination/source column {table.Name}.{destinationColumn.Name} type mismatch (source: {sourceColumn.Type} destination: {destinationColumn.Type})!");
                }

                if (sourceColumn.MaxLength > destinationColumn.MaxLength && destinationColumn.MaxLength != -1)
                {
                    throw new ArgumentException($"Source column {table.Name}.{destinationColumn.Name} has a longer length (source: {sourceColumn.MaxLength} destination: {destinationColumn.MaxLength})!");
                }

                if (sourceColumn.Precision == destinationColumn.Precision)
                {
                    
                }

                if (sourceColumn.Scale == destinationColumn.Scale)
                {

                }
            }

            return true;
        }

    }
}