using System.Data.SqlClient;
using System.Linq;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace DataTools.Sync.Core
{
    public interface IDbQueryFactory
    {
        QueryFactory GetSource(string synchronizationSet);
        QueryFactory GetDestination(string synchronizationSet);
    }

    public class DbQueryFactory : IDbQueryFactory
    {
        private readonly IAppConfig _config;

        public DbQueryFactory(IAppConfig config)
        {
            _config = config;
        }

        public QueryFactory GetSource(string synchronizationSet)
        {
            var connection = new SqlConnection(_config.Get().SynchronizationSets.Single(x=>x.Name == synchronizationSet).Source.ConnectionString);
            connection.Open();
            var compiler = new SqlServerCompiler();
            compiler.UseLegacyPagination = false;
            return new QueryFactory(connection, compiler);
        }

        public QueryFactory GetDestination(string synchronizationSet)
        {
            var connection = new SqlConnection(_config.Get().SynchronizationSets.Single(x => x.Name == synchronizationSet).Destination.ConnectionString);
            connection.Open();
            var compiler = new SqlServerCompiler();
            compiler.UseLegacyPagination = false;
            return new QueryFactory(connection, compiler);
        }
    }
}