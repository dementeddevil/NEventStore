namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Data.SqlClient;

    public class MsSqlDialect : CommonSqlDialect
    {
        private const int UniqueIndexViolation = 2601;
        private const int UniqueKeyViolation = 2627;

        public override string InitializeStorage => MsSqlStatements.InitializeStorage;

        public override string GetSnapshot => "SET ROWCOUNT 1;\n" + base.GetSnapshot.Replace("LIMIT 1;", ";");

        public override string GetCommitsFromStartingRevision => NaturalPaging(base.GetCommitsFromStartingRevision);

        public override string GetCommitsFromInstant => CommonTableExpressionPaging(base.GetCommitsFromInstant);

        public override string GetCommitsFromToInstant => CommonTableExpressionPaging(base.GetCommitsFromToInstant);

        public override string PersistCommit => MsSqlStatements.PersistCommits;

        public override string GetCommitsFromCheckpoint => CommonTableExpressionPaging(base.GetCommitsFromCheckpoint);

        public override string GetUndispatchedCommits => CommonTableExpressionPaging(base.GetUndispatchedCommits);

        public override string GetStreamsRequiringSnapshots => NaturalPaging(base.GetStreamsRequiringSnapshots);

        private static string NaturalPaging(string query)
        {
            return "SET ROWCOUNT @Limit;\n" + RemovePaging(query);
        }

        private static string CommonTableExpressionPaging(string query)
        {
            query = RemovePaging(query);
            var orderByIndex = query.IndexOf("ORDER BY");
            var orderBy = query.Substring(orderByIndex).Replace(";", string.Empty);
            query = query.Substring(0, orderByIndex);

            var fromIndex = query.IndexOf("FROM ");
            var from = query.Substring(fromIndex);
            var select = query.Substring(0, fromIndex);

            var value = MsSqlStatements.PagedQueryFormat.FormatWith(select, orderBy, from);
            return value;
        }

        private static string RemovePaging(string query)
        {
            return query
                .Replace("\n LIMIT @Limit OFFSET @Skip;", ";")
                .Replace("\n LIMIT @Limit;", ";");
        }

        public override bool IsDuplicate(Exception exception)
        {
            var dbException = exception as SqlException;
            return dbException != null &&
                   (dbException.Number == UniqueIndexViolation || dbException.Number == UniqueKeyViolation);
        }
    }
}