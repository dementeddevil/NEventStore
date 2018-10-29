namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;

    public class SqliteDialect : CommonSqlDialect
    {
        public override string InitializeStorage => SqliteStatements.InitializeStorage;

        // Sqlite wants all parameters to be a part of the query
        public override string GetCommitsFromStartingRevision => base.GetCommitsFromStartingRevision.Replace("\n ORDER BY ", "\n  AND @Skip = @Skip\nORDER BY ");

        public override string PersistCommit => SqliteStatements.PersistCommit;

        public override DateTime ToDateTime(object value)
        {
            return ((DateTime) value).ToUniversalTime();
        }
    }
}