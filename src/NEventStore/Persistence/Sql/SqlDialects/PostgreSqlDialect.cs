namespace NEventStore.Persistence.Sql.SqlDialects
{
    public class PostgreSqlDialect : CommonSqlDialect
    {
        public override string InitializeStorage => PostgreSqlStatements.InitializeStorage;

        public override string MarkCommitAsDispatched => base.MarkCommitAsDispatched.Replace("1", "true");

        public override string PersistCommit => PostgreSqlStatements.PersistCommits;

        public override string GetUndispatchedCommits => base.GetUndispatchedCommits.Replace("0", "false");
    }
}