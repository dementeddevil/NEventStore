namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using NEventStore.Logging;
    using NEventStore.Serialization;

    public class SqlPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (SqlPersistenceEngine));
        private static readonly DateTime EpochTime = new DateTime(1970, 1, 1);
        private readonly IConnectionFactory _connectionFactory;
        private readonly ISqlDialect _dialect;
        private readonly int _pageSize;
        private readonly TransactionScopeOption _scopeOption;
        private readonly ISerialize _serializer;
        private bool _disposed;
        private int _initialized;
        private readonly IStreamIdHasher _streamIdHasher;

        public SqlPersistenceEngine(
            IConnectionFactory connectionFactory,
            ISqlDialect dialect,
            ISerialize serializer,
            TransactionScopeOption scopeOption,
            int pageSize)
            : this(connectionFactory, dialect, serializer, scopeOption, pageSize, new Sha1StreamIdHasher())
        {}

        public SqlPersistenceEngine(
            IConnectionFactory connectionFactory,
            ISqlDialect dialect,
            ISerialize serializer,
            TransactionScopeOption scopeOption,
            int pageSize,
            IStreamIdHasher streamIdHasher)
        {
            if (connectionFactory == null)
            {
                throw new ArgumentNullException(nameof(connectionFactory));
            }

            if (dialect == null)
            {
                throw new ArgumentNullException(nameof(dialect));
            }

            if (serializer == null)
            {
                throw new ArgumentNullException(nameof(serializer));
            }

            if (pageSize < 0)
            {
                throw new ArgumentException("pageSize");
            }

            if (streamIdHasher == null)
            {
                throw new ArgumentNullException(nameof(streamIdHasher));
            }

            _connectionFactory = connectionFactory;
            _dialect = dialect;
            _serializer = serializer;
            _scopeOption = scopeOption;
            _pageSize = pageSize;
            _streamIdHasher = new StreamIdHasherValidator(streamIdHasher);

            Logger.Debug(Messages.UsingScope, _scopeOption.ToString());
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _initialized) > 1)
            {
                return Task.CompletedTask;
            }

            Logger.Debug(Messages.InitializingStorage);
            ExecuteCommand(statement => statement.ExecuteWithoutExceptions(_dialect.InitializeStorage, cancellationToken));
            return Task.CompletedTask;
        }

        public virtual Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.GettingAllCommitsBetween, streamId, minRevision, maxRevision);
            streamId = _streamIdHasher.GetHash(streamId);
            return ExecuteQuery(
                async query =>
                {
                    var statement = _dialect.GetCommitsFromStartingRevision;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamRevision, minRevision);
                    query.AddParameter(_dialect.MaxStreamRevision, maxRevision);
                    query.AddParameter(_dialect.CommitSequence, 0);
                    return (await query
                        .ExecutePagedQuery(statement, _dialect.NextPageDelegate, cancellationToken).ConfigureAwait(false))
                        .Select(x => x.GetCommit(_serializer, _dialect));
                });
        }

        public virtual Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, DateTime start, CancellationToken cancellationToken)
        {
            start = start.AddTicks(-(start.Ticks%TimeSpan.TicksPerSecond)); // Rounds down to the nearest second.
            start = start < EpochTime ? EpochTime : start;

            Logger.Debug(Messages.GettingAllCommitsFrom, start, bucketId);
            return ExecuteQuery(
                async query =>
                {
                    var statement = _dialect.GetCommitsFromInstant;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.CommitStamp, start);
                    return (await query.ExecutePagedQuery(statement, (q, r) => { }, cancellationToken).ConfigureAwait(false))
                            .Select(x => x.GetCommit(_serializer, _dialect));
                });
        }

        public Task<ICheckpoint> GetCheckpointAsync(CancellationToken cancellationToken, string checkpointToken)
        {
            return Task.FromResult<ICheckpoint>(string.IsNullOrWhiteSpace(checkpointToken) ? null : LongCheckpoint.Parse(checkpointToken));
        }

        public virtual Task<IEnumerable<ICommit>> GetFromToAsync(string bucketId, DateTime start, DateTime end, CancellationToken cancellationToken)
        {
            start = start.AddTicks(-(start.Ticks%TimeSpan.TicksPerSecond)); // Rounds down to the nearest second.
            start = start < EpochTime ? EpochTime : start;
            end = end < EpochTime ? EpochTime : end;

            Logger.Debug(Messages.GettingAllCommitsFromTo, start, end);
            return ExecuteQuery(
                async query =>
                {
                    var statement = _dialect.GetCommitsFromToInstant;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.CommitStampStart, start);
                    query.AddParameter(_dialect.CommitStampEnd, end);
                    return (await query.ExecutePagedQuery(statement, (q, r) => { }, cancellationToken).ConfigureAwait(false))
                        .Select(x => x.GetCommit(_serializer, _dialect));
                });
        }

        public virtual async Task<ICommit> CommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            ICommit commit;
            try
            {
                commit = await PersistCommitAsync(attempt, cancellationToken).ConfigureAwait(false);
                Logger.Debug(Messages.CommitPersisted, attempt.CommitId);
            }
            catch (Exception e)
            {
                if (!(e is UniqueKeyViolationException))
                {
                    throw;
                }

                if (await DetectDuplicateAsync(attempt, cancellationToken).ConfigureAwait(false))
                {
                    Logger.Info(Messages.DuplicateCommit);
                    throw new DuplicateCommitException(e.Message, e);
                }

                Logger.Info(Messages.ConcurrentWriteDetected);
                throw new ConcurrencyException(e.Message, e);
            }
            return commit;
        }

        public virtual Task<IEnumerable<ICommit>> GetUndispatchedCommitsAsync(CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.GettingUndispatchedCommits);
            return ExecuteQuery(
                async query => 
                    (await query.ExecutePagedQuery(_dialect.GetUndispatchedCommits, (q, r) => { }, cancellationToken).ConfigureAwait(false))
                    .Select(x => x.GetCommit(_serializer, _dialect))
                    .ToArray()
                    .AsEnumerable()); // avoid paging
        }

        public virtual Task MarkCommitAsDispatchedAsync(ICommit commit, CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.MarkingCommitAsDispatched, commit.CommitId);
            var streamId = _streamIdHasher.GetHash(commit.StreamId);
            return ExecuteCommand(cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, commit.BucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.CommitSequence, commit.CommitSequence);
                    return cmd.ExecuteWithoutExceptions(_dialect.MarkCommitAsDispatched, cancellationToken);
                });
        }

        public virtual Task<IEnumerable<IStreamHead>> GetStreamsToSnapshotAsync(string bucketId, int maxThreshold, CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.GettingStreamsToSnapshot);
            return ExecuteQuery(
                async query =>
                {
                    var statement = _dialect.GetStreamsRequiringSnapshots;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.Threshold, maxThreshold);
                    return (await query.ExecutePagedQuery(
                        statement,
                        (q, s) => q.SetParameter(_dialect.StreamId, _dialect.CoalesceParameterValue(s.StreamId()), DbType.AnsiString), cancellationToken).ConfigureAwait(false))
                            .Select(x => (IStreamHead)x.GetStreamToSnapshot());
                });
        }

        public virtual async Task<ISnapshot> GetSnapshotAsync(string bucketId, string streamId, int maxRevision, CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.GettingRevision, streamId, maxRevision);
            streamId = _streamIdHasher.GetHash(streamId);
            return (await ExecuteQuery(
                async query =>
                {
                    var statement = _dialect.GetSnapshot;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamRevision, maxRevision);
                    return (await query.ExecuteWithQuery(statement, cancellationToken).ConfigureAwait(false))
                        .Select(x => x.GetSnapshot(_serializer));
                }).ConfigureAwait(false))
                .FirstOrDefault<ISnapshot>();
        }

        public virtual Task<bool> AddSnapshotAsync(ISnapshot snapshot, CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.AddingSnapshot, snapshot.StreamId, snapshot.StreamRevision);
            var streamId = _streamIdHasher.GetHash(snapshot.StreamId);
            return ExecuteCommand(
                async (connection, cmd) =>
                {
                    cmd.AddParameter(_dialect.BucketId, snapshot.BucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamRevision, snapshot.StreamRevision);
                    _dialect.AddPayloadParamater(_connectionFactory, connection, cmd, _serializer.Serialize(snapshot.Payload));
                    return (await cmd.ExecuteWithoutExceptions(_dialect.AppendSnapshotToCommit, cancellationToken).ConfigureAwait(false)) > 0;
                });
        }

        public virtual Task PurgeAsync(CancellationToken cancellationToken)
        {
            Logger.Warn(Messages.PurgingStorage);
            return ExecuteCommand(cmd => cmd.ExecuteNonQuery(_dialect.PurgeStorage, cancellationToken));
        }

        public Task PurgeAsync(string bucketId, CancellationToken cancellationToken)
        {
            Logger.Warn(Messages.PurgingBucket, bucketId);
            return ExecuteCommand(cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    return cmd.ExecuteNonQuery(_dialect.PurgeBucket, cancellationToken);
                });
        }

        public Task DropAsync(CancellationToken cancellationToken)
        {
            Logger.Warn(Messages.DroppingTables);
            return ExecuteCommand(cmd => cmd.ExecuteNonQuery(_dialect.Drop, cancellationToken));
        }

        public Task DeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            Logger.Warn(Messages.DeletingStream, streamId, bucketId);
            streamId = _streamIdHasher.GetHash(streamId);
            return ExecuteCommand(cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    return cmd.ExecuteNonQuery(_dialect.DeleteStream, cancellationToken);
                });
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(CancellationToken cancellationToken, string checkpointToken)
        {
            var checkpoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug(Messages.GettingAllCommitsFromCheckpoint, checkpointToken);
            return ExecuteQuery(
                async query =>
                {
                    var statement = _dialect.GetCommitsFromCheckpoint;
                    query.AddParameter(_dialect.CheckpointNumber, checkpoint.LongValue);
                    return (await query.ExecutePagedQuery(
                            statement,
                            (q, r) =>
                            {
                            },
                            cancellationToken).ConfigureAwait(false))
                        .Select(x => x.GetCommit(_serializer, _dialect));
                });
        }

        public bool IsDisposed => _disposed;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            Logger.Debug(Messages.ShuttingDownPersistence);
            _disposed = true;
        }

        protected virtual void OnPersistCommit(IDbStatement cmd, CommitAttempt attempt)
        {}

        private Task<ICommit> PersistCommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            Logger.Debug(Messages.AttemptingToCommit, attempt.Events.Count, attempt.StreamId, attempt.CommitSequence, attempt.BucketId);
            var streamId = _streamIdHasher.GetHash(attempt.StreamId);
            return ExecuteCommand(
                async (connection, cmd) =>
                {
                    cmd.AddParameter(_dialect.BucketId, attempt.BucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamIdOriginal, attempt.StreamId);
                    cmd.AddParameter(_dialect.StreamRevision, attempt.StreamRevision);
                    cmd.AddParameter(_dialect.Items, attempt.Events.Count);
                    cmd.AddParameter(_dialect.CommitId, attempt.CommitId);
                    cmd.AddParameter(_dialect.CommitSequence, attempt.CommitSequence);
                    cmd.AddParameter(_dialect.CommitStamp, attempt.CommitStamp);
                    cmd.AddParameter(_dialect.Headers, _serializer.Serialize(attempt.Headers));
                    _dialect.AddPayloadParamater(_connectionFactory, connection, cmd, _serializer.Serialize(attempt.Events.ToList()));
                    OnPersistCommit(cmd, attempt);
                    var checkpointNumber = (await cmd.ExecuteScalar(_dialect.PersistCommit, cancellationToken).ConfigureAwait(false)).ToLong();
                    return (ICommit)
                        new Commit(
                            attempt.BucketId,
                            attempt.StreamId,
                            attempt.StreamRevision,
                            attempt.CommitId,
                            attempt.CommitSequence,
                            attempt.CommitStamp,
                            checkpointNumber.ToString(CultureInfo.InvariantCulture),
                            attempt.Headers,
                            attempt.Events);
                });
        }

        private Task<bool> DetectDuplicateAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            var streamId = _streamIdHasher.GetHash(attempt.StreamId);
            return ExecuteCommand(
                async cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, attempt.BucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.CommitId, attempt.CommitId);
                    cmd.AddParameter(_dialect.CommitSequence, attempt.CommitSequence);
                    var value = await cmd.ExecuteScalar(_dialect.DuplicateCommit, cancellationToken).ConfigureAwait(false);
                    return (value is long ? (long) value : (int) value) > 0;
                });
        }

        protected virtual Task<IEnumerable<T>> ExecuteQuery<T>(Func<IDbStatement, Task<IEnumerable<T>>> query)
        {
            ThrowWhenDisposed();

            var scope = OpenQueryScope();
            IDbConnection connection = null;
            IDbTransaction transaction = null;
            IDbStatement statement = null;

            try
            {
                connection = _connectionFactory.Open();
                transaction = _dialect.OpenTransaction(connection);
                statement = _dialect.BuildStatement(scope, connection, transaction);
                statement.PageSize = _pageSize;

                Logger.Verbose(Messages.ExecutingQuery);
                return query(statement);
            }
            catch (Exception e)
            {
                statement?.Dispose();
                transaction?.Dispose();
                connection?.Dispose();
                scope?.Dispose();

                Logger.Debug(Messages.StorageThrewException, e.GetType());
                if (e is StorageUnavailableException)
                {
                    throw;
                }

                throw new StorageException(e.Message, e);
            }
        }

        protected virtual TransactionScope OpenQueryScope()
        {
            return OpenCommandScope() ?? new TransactionScope(TransactionScopeOption.Suppress);
        }

        private void ThrowWhenDisposed()
        {
            if (!_disposed)
            {
                return;
            }

            Logger.Warn(Messages.AlreadyDisposed);
            throw new ObjectDisposedException(Messages.AlreadyDisposed);
        }

        private Task<T> ExecuteCommand<T>(Func<IDbStatement, Task<T>> command)
        {
            return ExecuteCommand((_, statement) => command(statement));
        }

        protected virtual Task<T> ExecuteCommand<T>(Func<IDbConnection, IDbStatement, Task<T>> command)
        {
            ThrowWhenDisposed();

            using (var scope = OpenCommandScope())
            using (var connection = _connectionFactory.Open())
            using (var transaction = _dialect.OpenTransaction(connection))
            using (var statement = _dialect.BuildStatement(scope, connection, transaction))
            {
                try
                {
                    Logger.Verbose(Messages.ExecutingCommand);
                    var rowsAffected = command(connection, statement);
                    Logger.Verbose(Messages.CommandExecuted, rowsAffected);

                    transaction?.Commit();

                    scope?.Complete();

                    return rowsAffected;
                }
                catch (Exception e)
                {
                    Logger.Debug(Messages.StorageThrewException, e.GetType());
                    if (!RecoverableException(e))
                    {
                        throw new StorageException(e.Message, e);
                    }

                    Logger.Info(Messages.RecoverableExceptionCompletesScope);
                    scope?.Complete();

                    throw;
                }
            }
        }

        protected virtual TransactionScope OpenCommandScope()
        {
            return new TransactionScope(_scopeOption);
        }

        private static bool RecoverableException(Exception e)
        {
            return e is UniqueKeyViolationException || e is StorageUnavailableException;
        }     

        private class StreamIdHasherValidator : IStreamIdHasher
        {
            private readonly IStreamIdHasher _streamIdHasher;
            private const int MaxStreamIdHashLength = 40;

            public StreamIdHasherValidator(IStreamIdHasher streamIdHasher)
            {
                if (streamIdHasher == null)
                {
                    throw new ArgumentNullException(nameof(streamIdHasher));
                }
                _streamIdHasher = streamIdHasher;
            }
            public string GetHash(string streamId)
            {
                if (string.IsNullOrWhiteSpace(streamId))
                {
                    throw new ArgumentException(Messages.StreamIdIsNullEmptyOrWhiteSpace);
                }
                var streamIdHash = _streamIdHasher.GetHash(streamId);
                if (string.IsNullOrWhiteSpace(streamIdHash))
                {
                    throw new InvalidOperationException(Messages.StreamIdHashIsNullEmptyOrWhiteSpace);
                }
                if (streamIdHash.Length > MaxStreamIdHashLength)
                {
                    throw new InvalidOperationException(Messages.StreamIdHashTooLong.FormatWith(streamId, streamIdHash, streamIdHash.Length, MaxStreamIdHashLength));
                }
                return streamIdHash;
            }
        }
    }
}