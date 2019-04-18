using System.Collections;
using System.Collections.Generic;

namespace DataTools.Sync.Model.Schema
{
    public class TableSchema
    {
        public int ObjectId { get; set; }
        public string Name { get; set; }
        public string Schema { get; set; }

        public List<ColumnSchema> Columns { get; set; }
    }
}