namespace DataTools.Sync.Model.Configuration
{
    public class Table
    {
        public string Name { get; set; }
        public string SourceFrom { get; set; }
        public string[] SortColumns { get; set; }
        public string SourceFilter { get; set; }

        public string From { get; set; }
        public string Filter { get; set; }
    }
}