using System.Data.SqlClient;
using System.Linq;
using Microsoft.Extensions.Configuration;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace DataTools.Sync.Core
{
    public interface IDbConnectionFactory
    {
        QueryFactory GetSource(string synchronizationSet);
        QueryFactory GetDestination(string synchronizationSet);
    }

    public class DbConnectionFactory : IDbConnectionFactory
    {
        private readonly IAppConfig _config;

        public DbConnectionFactory(IAppConfig config)
        {
            _config = config;
        }

        public QueryFactory GetSource(string synchronizationSet)
        {
            var connection = new SqlConnection(_config.Get().SynchronizationSets.Single(x=>x.Name == synchronizationSet).Source.ConnectionString);
            connection.Open();
            var compiler = new SqlServerCompiler();
            return new QueryFactory(connection, compiler);
        }

        public QueryFactory GetDestination(string synchronizationSet)
        {
            var connection = new SqlConnection(_config.Get().SynchronizationSets.Single(x => x.Name == synchronizationSet).Destination.ConnectionString);
            connection.Open();
            var compiler = new SqlServerCompiler();
            return new QueryFactory(connection, compiler);
        }
    }
}