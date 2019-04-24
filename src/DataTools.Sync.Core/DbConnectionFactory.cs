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
            var config = _config.Get().SynchronizationSets.Single(x => x.Name == synchronizationSet);

            var connection = new SqlConnection(config.Source.ConnectionString);
            connection.Open();
            var compiler = new SqlServerCompiler();
            compiler.UseLegacyPagination = false;
            var queryFactory = new QueryFactory(connection, compiler);
            queryFactory.QueryTimeout = config.Source.Timeout ?? 30;
            return queryFactory;
        }

        public QueryFactory GetDestination(string synchronizationSet)
        {
            var config = _config.Get().SynchronizationSets.Single(x => x.Name == synchronizationSet);

            var connection = new SqlConnection(config.Destination.ConnectionString);
            connection.Open();
            var compiler = new SqlServerCompiler();
            compiler.UseLegacyPagination = false;
            var queryFactory = new QueryFactory(connection, compiler);
            queryFactory.QueryTimeout = config.Source.Timeout ?? 30;
            return queryFactory;
        }
    }
}