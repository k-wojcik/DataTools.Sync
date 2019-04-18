namespace DataTools.Sync.Model.Schema
{
    public class ColumnSchema
    {
        public int ColumnId { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsNullable { get; set; }
        public bool IsComputed { get; set; }
        public short MaxLength { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }

        public bool IsPrimaryKey { get; set; }
    }
}