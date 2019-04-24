namespace DataTools.Sync.Model.Configuration
{
    public class Destination
    {
        public string Provider { get; set; }
        public string ConnectionString { get; set; }
        public int Timeout { get; set; }
    }
}