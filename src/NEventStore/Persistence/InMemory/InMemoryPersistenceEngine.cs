using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Logging;

namespace NEventStore.Persistence.InMemory
{
    public class InMemoryPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (InMemoryPersistenceEngine));
        private readonly ConcurrentDictionary<string, Bucket> _buckets = new ConcurrentDictionary<string, Bucket>();
        private bool _disposed;
        private int _checkpoint;

        private Bucket this[string bucketId]
        {
            get { return _buckets.GetOrAdd(bucketId, _ => new Bucket()); }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public Task InitializeAsync(CancellationToken cancellationToken)
        {
            Logger.Info(Resources.InitializingEngine);
            return Task.CompletedTask;
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.GettingAllCommitsFromRevision, streamId, minRevision, maxRevision);
            return Task.FromResult(this[bucketId].GetFrom(streamId, minRevision, maxRevision));
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, DateTime start, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.GettingAllCommitsFromTime, bucketId, start);
            return Task.FromResult(this[bucketId].GetFrom(start));
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(CancellationToken cancellationToken, string checkpointToken)
        {
            Logger.Debug(Resources.GettingAllCommitsFromCheckpoint, checkpointToken);
            ICheckpoint checkpoint = LongCheckpoint.Parse(checkpointToken);
            return Task.FromResult<IEnumerable<ICommit>>(_buckets
                .Values
                .SelectMany(b => b.GetCommits())
                .Where(c => c.Checkpoint.CompareTo(checkpoint) > 0)
                .OrderBy(c => c.Checkpoint)
                .ToArray());
        }

        public Task<ICheckpoint> GetCheckpointAsync(CancellationToken cancellationToken, string checkpointToken = null)
        {
            return Task.FromResult<ICheckpoint>(LongCheckpoint.Parse(checkpointToken));
        }

        public Task<IEnumerable<ICommit>> GetFromToAsync(string bucketId, DateTime start, DateTime end, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.GettingAllCommitsFromToTime, start, end);
            return Task.FromResult(this[bucketId].GetFromTo(start, end));
        }

        public Task<ICommit> CommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.AttemptingToCommit, attempt.CommitId, attempt.StreamId, attempt.CommitSequence);
            return Task.FromResult(this[attempt.BucketId].Commit(attempt, new LongCheckpoint(Interlocked.Increment(ref _checkpoint))));
        }

        public Task<IEnumerable<ICommit>> GetUndispatchedCommitsAsync(CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            return Task.FromResult(_buckets.Values.SelectMany(b => b.GetUndispatchedCommits()));
        }

        public Task MarkCommitAsDispatchedAsync(ICommit commit, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.MarkingAsDispatched, commit.CommitId);
            this[commit.BucketId].MarkCommitAsDispatched(commit);
            return Task.CompletedTask;
        }

        public Task<IEnumerable<IStreamHead>> GetStreamsToSnapshotAsync(string bucketId, int maxThreshold, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.GettingStreamsToSnapshot, bucketId, maxThreshold);
            return Task.FromResult(this[bucketId].GetStreamsToSnapshot(maxThreshold));
        }

        public Task<ISnapshot> GetSnapshotAsync(string bucketId, string streamId, int maxRevision, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.GettingSnapshotForStream, bucketId, streamId, maxRevision);
            return Task.FromResult(this[bucketId].GetSnapshot(streamId, maxRevision));
        }

        public Task<bool> AddSnapshotAsync(ISnapshot snapshot, CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Debug(Resources.AddingSnapshot, snapshot.StreamId, snapshot.StreamRevision);
            return Task.FromResult(this[snapshot.BucketId].AddSnapshot(snapshot));
        }

        public Task PurgeAsync(CancellationToken cancellationToken)
        {
            ThrowWhenDisposed();
            Logger.Warn(Resources.PurgingStore);
            foreach (var bucket in _buckets.Values)
            {
                bucket.Purge();
            }

            return Task.CompletedTask;
        }

        public Task PurgeAsync(string bucketId, CancellationToken cancellationToken)
        {
            Bucket _;
            _buckets.TryRemove(bucketId, out _);
            return Task.CompletedTask;
        }

        public Task DropAsync(CancellationToken cancellationToken)
        {
            _buckets.Clear();
            return Task.CompletedTask;
        }

        public Task DeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            Logger.Warn(Resources.DeletingStream, streamId, bucketId);
            if (_buckets.TryGetValue(bucketId, out var bucket))
            {
                bucket.DeleteStream(streamId);
            }
            return Task.CompletedTask;
        }

        public bool IsDisposed => _disposed;

        // ReSharper disable once UnusedParameter.Local
        private void Dispose(bool disposing)
        {
            _disposed = true;
            Logger.Info(Resources.DisposingEngine);
        }

        private void ThrowWhenDisposed()
        {
            if (!_disposed)
            {
                return;
            }

            Logger.Warn(Resources.AlreadyDisposed);
            throw new ObjectDisposedException(Resources.AlreadyDisposed);
        }

        private class InMemoryCommit : Commit
        {
            public InMemoryCommit(
                string bucketId,
                string streamId,
                int streamRevision,
                Guid commitId,
                int commitSequence,
                DateTime commitStamp,
                string checkpointToken,
                IDictionary<string, object> headers,
                IEnumerable<EventMessage> events,
                ICheckpoint checkpoint)
                : base(bucketId, streamId, streamRevision, commitId, commitSequence, commitStamp, checkpointToken, headers, events)
            {
                Checkpoint = checkpoint;
            }

            public ICheckpoint Checkpoint { get; }
        }

        private class IdentityForDuplicationDetection
        {
            protected bool Equals(IdentityForDuplicationDetection other)
            {
                return string.Equals(_streamId, other._streamId) && string.Equals(_bucketId, other._bucketId) && _commitSequence == other._commitSequence && _commitId.Equals(other._commitId);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                {
                    return false;
                }
                if (ReferenceEquals(this, obj))
                {
                    return true;
                }
                if (obj.GetType() != GetType())
                {
                    return false;
                }
                return Equals((IdentityForDuplicationDetection)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = _streamId.GetHashCode();
                    hashCode = (hashCode * 397) ^ _bucketId.GetHashCode();
                    hashCode = (hashCode * 397) ^ _commitSequence;
                    hashCode = (hashCode * 397) ^ _commitId.GetHashCode();
                    return hashCode;
                }
            }

            private readonly int _commitSequence;
            private readonly Guid _commitId;
            private readonly string _bucketId;
            private readonly string _streamId;

            public IdentityForDuplicationDetection(CommitAttempt commitAttempt)
            {
                _bucketId = commitAttempt.BucketId;
                _streamId = commitAttempt.StreamId;
                _commitId = commitAttempt.CommitId;
                _commitSequence = commitAttempt.CommitSequence;
            }

            public IdentityForDuplicationDetection(Commit commit)
            {
                _bucketId = commit.BucketId;
                _streamId = commit.StreamId;
                _commitId = commit.CommitId;
                _commitSequence = commit.CommitSequence;
            }
        }

        private class IdentityForConcurrencyConflictDetection
        {
            protected bool Equals(IdentityForConcurrencyConflictDetection other)
            {
                return _commitSequence == other._commitSequence && string.Equals(_streamId, other._streamId);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                {
                    return false;
                }
                if (ReferenceEquals(this, obj))
                {
                    return true;
                }
                if (obj.GetType() != GetType())
                {
                    return false;
                }
                return Equals((IdentityForConcurrencyConflictDetection)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (_commitSequence * 397) ^ _streamId.GetHashCode();
                }
            }

            private readonly int _commitSequence;

            private readonly string _streamId;

            public IdentityForConcurrencyConflictDetection(Commit commit)
            {
                _streamId = commit.StreamId;
                _commitSequence = commit.CommitSequence;
            }
        }

        private class Bucket
        {
            private readonly IList<InMemoryCommit> _commits = new List<InMemoryCommit>();
            private readonly ICollection<IdentityForDuplicationDetection> _potentialDuplicates = new HashSet<IdentityForDuplicationDetection>();
            private readonly ICollection<IdentityForConcurrencyConflictDetection> _potentialConflicts = new HashSet<IdentityForConcurrencyConflictDetection>(); 

            public IEnumerable<InMemoryCommit> GetCommits()
            {
                lock (_commits)
                {
                    return _commits.ToArray();
                }
            }

            private readonly ICollection<IStreamHead> _heads = new LinkedList<IStreamHead>();
            private readonly ICollection<ISnapshot> _snapshots = new LinkedList<ISnapshot>();
            private readonly IDictionary<Guid, DateTime> _stamps = new Dictionary<Guid, DateTime>();
            private readonly ICollection<ICommit> _undispatched = new LinkedList<ICommit>();

            public IEnumerable<ICommit> GetFrom(string streamId, int minRevision, int maxRevision)
            {
                lock (_commits)
                {
                    return _commits
                        .Where(x => x.StreamId == streamId && x.StreamRevision >= minRevision && (x.StreamRevision - x.Events.Count + 1) <= maxRevision)
                        .OrderBy(c => c.CommitSequence)
                        .ToArray();
                }
            }

            public IEnumerable<ICommit> GetFrom(DateTime start)
            {
                var commitId = _stamps.Where(x => x.Value >= start).Select(x => x.Key).FirstOrDefault();
                if (commitId == Guid.Empty)
                {
                    return Enumerable.Empty<ICommit>();
                }

                var startingCommit = _commits.FirstOrDefault(x => x.CommitId == commitId);
                return _commits.Skip(_commits.IndexOf(startingCommit));
            }

            public IEnumerable<ICommit> GetFromTo(DateTime start, DateTime end)
            {
                IEnumerable<Guid> selectedCommitIds = _stamps.Where(x => x.Value >= start && x.Value < end).Select(x => x.Key).ToArray();
                var firstCommitId = selectedCommitIds.FirstOrDefault();
                var lastCommitId = selectedCommitIds.LastOrDefault();
                if (lastCommitId == Guid.Empty && lastCommitId == Guid.Empty)
                {
                    return Enumerable.Empty<ICommit>();
                }
                var startingCommit = _commits.FirstOrDefault(x => x.CommitId == firstCommitId);
                var endingCommit = _commits.FirstOrDefault(x => x.CommitId == lastCommitId);
                var startingCommitIndex = (startingCommit == null) ? 0 : _commits.IndexOf(startingCommit);
                var endingCommitIndex = (endingCommit == null) ? _commits.Count - 1 : _commits.IndexOf(endingCommit);
                var numberToTake = endingCommitIndex - startingCommitIndex + 1;

                return _commits.Skip(_commits.IndexOf(startingCommit)).Take(numberToTake);
            }

            public ICommit Commit(CommitAttempt attempt, ICheckpoint checkpoint)
            {
                lock (_commits)
                {
                    DetectDuplicate(attempt);
                    var commit = new InMemoryCommit(attempt.BucketId,
                        attempt.StreamId,
                        attempt.StreamRevision,
                        attempt.CommitId,
                        attempt.CommitSequence,
                        attempt.CommitStamp,
                        checkpoint.Value,
                        attempt.Headers,
                        attempt.Events,
                        checkpoint);
                    if (_potentialConflicts.Contains(new IdentityForConcurrencyConflictDetection(commit)))
                    {
                        throw new ConcurrencyException();
                    }
                    _stamps[commit.CommitId] = commit.CommitStamp;
                    _commits.Add(commit);
                    _potentialDuplicates.Add(new IdentityForDuplicationDetection(commit));
                    _potentialConflicts.Add(new IdentityForConcurrencyConflictDetection(commit));
                    _undispatched.Add(commit);
                    var head = _heads.FirstOrDefault(x => x.StreamId == commit.StreamId);
                    _heads.Remove(head);
                    Logger.Debug(Resources.UpdatingStreamHead, commit.StreamId);
                    var snapshotRevision = head == null ? 0 : head.SnapshotRevision;
                    _heads.Add(new StreamHead(commit.BucketId, commit.StreamId, commit.StreamRevision, snapshotRevision));
                    return commit;
                }
            }

            private void DetectDuplicate(CommitAttempt attempt)
            {
                if (_potentialDuplicates.Contains(new IdentityForDuplicationDetection(attempt)))
                {
                    throw new DuplicateCommitException();
                }
            }

            public IEnumerable<ICommit> GetUndispatchedCommits()
            {
                lock (_commits)
                {
                    Logger.Debug(Resources.RetrievingUndispatchedCommits, _commits.Count);
                    return _commits.Where(c => _undispatched.Contains(c)).OrderBy(c => c.CommitSequence);
                }
            }

            public void MarkCommitAsDispatched(ICommit commit)
            {
                lock (_commits)
                {
                    _undispatched.Remove(commit);
                }
            }

            public IEnumerable<IStreamHead> GetStreamsToSnapshot(int maxThreshold)
            {
                lock (_commits)
                {
                    return _heads
                        .Where(x => x.HeadRevision >= x.SnapshotRevision + maxThreshold)
                        .Select(stream => new StreamHead(stream.BucketId, stream.StreamId, stream.HeadRevision, stream.SnapshotRevision));
                }
            }

            public ISnapshot GetSnapshot(string streamId, int maxRevision)
            {
                lock (_commits)
                {
                    return _snapshots
                        .Where(x => x.StreamId == streamId && x.StreamRevision <= maxRevision)
                        .OrderByDescending(x => x.StreamRevision)
                        .FirstOrDefault();
                }
            }

            public bool AddSnapshot(ISnapshot snapshot)
            {
                lock (_commits)
                {
                    var currentHead = _heads.FirstOrDefault(h => h.StreamId == snapshot.StreamId);
                    if (currentHead == null)
                    {
                        return false;
                    }

                    _snapshots.Add(snapshot);
                    _heads.Remove(currentHead);
                    _heads.Add(new StreamHead(currentHead.BucketId, currentHead.StreamId, currentHead.HeadRevision, snapshot.StreamRevision));
                }
                return true;
            }

            public void Purge()
            {
                lock (_commits)
                {
                    _commits.Clear();
                    _snapshots.Clear();
                    _heads.Clear();
                    _potentialConflicts.Clear();
                    _potentialDuplicates.Clear();
                }
            }

            public void DeleteStream(string streamId)
            {
                lock (_commits)
                {
                    var commits = _commits.Where(c => c.StreamId == streamId).ToArray();
                    foreach (var commit in commits)
                    {
                        _commits.Remove(commit);
                    }
                    var snapshots = _snapshots.Where(s => s.StreamId == streamId).ToArray();
                    foreach (var snapshot in snapshots)
                    {
                        _snapshots.Remove(snapshot);
                    }
                    var streamHead = _heads.SingleOrDefault(s => s.StreamId == streamId);
                    if (streamHead != null)
                    {
                        _heads.Remove(streamHead);
                    }
                }
            }
        }
    }
}