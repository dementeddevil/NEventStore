using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Persistence;

namespace NEventStore.Diagnostics
{
    public class PerformanceCounterPersistenceEngine : IPersistStreams
    {
        private readonly PerformanceCounters _counters;
        private readonly IPersistStreams _persistence;

        public PerformanceCounterPersistenceEngine(IPersistStreams persistence, string instanceName)
        {
            _persistence = persistence;
            _counters = new PerformanceCounters(instanceName);
        }

        public Task InitializeAsync(CancellationToken cancellationToken)
        {
            return _persistence.InitializeAsync(cancellationToken);
        }

        public async Task<ICommit> CommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            var clock = Stopwatch.StartNew();

            var commit = await _persistence
                .CommitAsync(attempt, cancellationToken)
                .ConfigureAwait(false);

            clock.Stop();
            _counters.CountCommit(attempt.Events.Count, clock.ElapsedMilliseconds);

            return commit;
        }

        public async Task MarkCommitAsDispatchedAsync(ICommit commit, CancellationToken cancellationToken)
        {
            await _persistence
                .MarkCommitAsDispatchedAsync(commit, cancellationToken)
                .ConfigureAwait(false);

            _counters.CountCommitDispatched();
        }

        public ICheckpoint ParseCheckpoint(string checkpointValue)
        {
            return LongCheckpoint.Parse(checkpointValue);
        }

        public Task<ICheckpoint> GetCheckpointAsync(CancellationToken cancellationToken, string checkpointToken = null)
        {
            return _persistence.GetCheckpointAsync(cancellationToken, checkpointToken);
        }

        public Task<IEnumerable<ICommit>> GetFromToAsync(string bucketId, DateTime start, DateTime end, CancellationToken cancellationToken)
        {
            return _persistence.GetFromToAsync(bucketId, start, end, cancellationToken);
        }

        public Task<IEnumerable<ICommit>> GetUndispatchedCommitsAsync(CancellationToken cancellationToken)
        {
            return _persistence.GetUndispatchedCommitsAsync(cancellationToken);
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            return _persistence.GetFromAsync(bucketId, streamId, minRevision, maxRevision, cancellationToken);
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, DateTime start, CancellationToken cancellationToken)
        {
            return _persistence.GetFromAsync(bucketId, start, cancellationToken);
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(CancellationToken cancellationToken, string checkpointToken)
        {
            return _persistence.GetFromAsync(cancellationToken, checkpointToken);
        }

        public async Task<bool> AddSnapshotAsync(ISnapshot snapshot, CancellationToken cancellationToken)
        {
            var result = await _persistence
                .AddSnapshotAsync(snapshot, cancellationToken)
                .ConfigureAwait(false);
            if (result)
            {
                _counters.CountSnapshot();
            }

            return result;
        }

        public Task<ISnapshot> GetSnapshotAsync(string bucketId, string streamId, int maxRevision, CancellationToken cancellationToken)
        {
            return _persistence.GetSnapshotAsync(bucketId, streamId, maxRevision, cancellationToken);
        }

        public virtual Task<IEnumerable<IStreamHead>> GetStreamsToSnapshotAsync(string bucketId, int maxThreshold, CancellationToken cancellationToken)
        {
            return _persistence.GetStreamsToSnapshotAsync(bucketId, maxThreshold, cancellationToken);
        }

        public virtual Task PurgeAsync(CancellationToken cancellationToken)
        {
            return _persistence.PurgeAsync(cancellationToken);
        }

        public Task PurgeAsync(string bucketId, CancellationToken cancellationToken)
        {
            return _persistence.PurgeAsync(bucketId, cancellationToken);
        }

        public Task DropAsync(CancellationToken cancellationToken)
        {
            return _persistence.DropAsync(cancellationToken);
        }

        public Task DeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            return _persistence.DeleteStreamAsync(bucketId, streamId, cancellationToken);
        }

        public bool IsDisposed => _persistence.IsDisposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~PerformanceCounterPersistenceEngine()
        {
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            _counters.Dispose();
            _persistence.Dispose();
        }

        public IPersistStreams UnwrapPersistenceEngine()
        {
            return _persistence;
        }
    }
}