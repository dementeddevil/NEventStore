namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Data;
    using System.Transactions;
    using NEventStore.Persistence.Sql;

    public abstract class CommonSqlDialect : ISqlDialect
    {
        public abstract string InitializeStorage { get; }

        public virtual string PurgeStorage => CommonSqlStatements.PurgeStorage;

        public string PurgeBucket => CommonSqlStatements.PurgeBucket;

        public virtual string Drop => CommonSqlStatements.DropTables;

        public virtual string DeleteStream => CommonSqlStatements.DeleteStream;

        public virtual string GetCommitsFromStartingRevision => CommonSqlStatements.GetCommitsFromStartingRevision;

        public virtual string GetCommitsFromInstant => CommonSqlStatements.GetCommitsFromInstant;

        public virtual string GetCommitsFromToInstant => CommonSqlStatements.GetCommitsFromToInstant;

        public abstract string PersistCommit { get; }

        public virtual string DuplicateCommit => CommonSqlStatements.DuplicateCommit;

        public virtual string GetStreamsRequiringSnapshots => CommonSqlStatements.GetStreamsRequiringSnapshots;

        public virtual string GetSnapshot => CommonSqlStatements.GetSnapshot;

        public virtual string AppendSnapshotToCommit => CommonSqlStatements.AppendSnapshotToCommit;

        public virtual string GetUndispatchedCommits => CommonSqlStatements.GetUndispatchedCommits;

        public virtual string MarkCommitAsDispatched => CommonSqlStatements.MarkCommitAsDispatched;

        public virtual string BucketId => "@BucketId";

        public virtual string StreamId => "@StreamId";

        public virtual string StreamIdOriginal => "@StreamIdOriginal";

        public virtual string StreamRevision => "@StreamRevision";

        public virtual string MaxStreamRevision => "@MaxStreamRevision";

        public virtual string Items => "@Items";

        public virtual string CommitId => "@CommitId";

        public virtual string CommitSequence => "@CommitSequence";

        public virtual string CommitStamp => "@CommitStamp";

        public virtual string CommitStampStart => "@CommitStampStart";

        public virtual string CommitStampEnd => "@CommitStampEnd";

        public virtual string Headers => "@Headers";

        public virtual string Payload => "@Payload";

        public virtual string Threshold => "@Threshold";

        public virtual string Limit => "@Limit";

        public virtual string Skip => "@Skip";

        public virtual bool CanPage => true;

        public virtual string CheckpointNumber => "@CheckpointNumber";

        public virtual string GetCommitsFromCheckpoint => CommonSqlStatements.GetCommitsFromCheckpoint;

        public virtual object CoalesceParameterValue(object value)
        {
            return value;
        }

        public virtual bool IsDuplicate(Exception exception)
        {
            var message = exception.Message.ToUpperInvariant();
            return message.Contains("DUPLICATE") || message.Contains("UNIQUE") || message.Contains("CONSTRAINT");
        }

        public virtual void AddPayloadParamater(IConnectionFactory connectionFactory, IDbConnection connection, IDbStatement cmd, byte[] payload)
        {
            cmd.AddParameter(Payload, payload);
        }

        public virtual DateTime ToDateTime(object value)
        {
            value = value is decimal ? (long) (decimal) value : value;
            return value is long ? new DateTime((long) value) : DateTime.SpecifyKind((DateTime) value, DateTimeKind.Utc);
        }

        public virtual NextPageDelegate NextPageDelegate
        {
            get { return (q, r) => q.SetParameter(CommitSequence, r.CommitSequence()); }
        }

        public virtual IDbTransaction OpenTransaction(IDbConnection connection)
        {
            return null;
        }

        public virtual IDbStatement BuildStatement(
            TransactionScope scope, IDbConnection connection, IDbTransaction transaction)
        {
            return new CommonDbStatement(this, scope, connection, transaction);
        }
    }
}