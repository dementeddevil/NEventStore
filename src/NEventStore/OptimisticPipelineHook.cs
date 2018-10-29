using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Logging;
using NEventStore.Persistence;

namespace NEventStore
{
    /// <summary>
    /// Tracks the heads of streams to reduce latency by avoiding round-trip
    /// to underlying storage.
    /// </summary>
    public class OptimisticPipelineHook : PipelineHookBase
    {
        private const int MaxStreamsToTrack = 100;
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (OptimisticPipelineHook));
        private readonly Dictionary<HeadKey, ICommit> _heads = new Dictionary<HeadKey, ICommit>(); //TODO use concurrent collections
        private readonly LinkedList<HeadKey> _maxItemsToTrack = new LinkedList<HeadKey>();
        private readonly int _maxStreamsToTrack;

        public OptimisticPipelineHook()
            : this(MaxStreamsToTrack)
        {}

        public OptimisticPipelineHook(int maxStreamsToTrack)
        {
            Logger.Debug(Resources.TrackingStreams, maxStreamsToTrack);
            _maxStreamsToTrack = maxStreamsToTrack;
        }

        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public override Task<ICommit> SelectAsync(ICommit committed, CancellationToken cancellationToken)
        {
            Track(committed);
            return Task.FromResult(committed);
        }

        public override Task<bool> PreCommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            Logger.Debug(Resources.OptimisticConcurrencyCheck, attempt.StreamId);

            var head = GetStreamHead(GetHeadKey(attempt));
            if (head == null)
            {
                return Task.FromResult(true);
            }

            if (head.CommitSequence >= attempt.CommitSequence)
            {
                throw new ConcurrencyException();
            }

            if (head.StreamRevision >= attempt.StreamRevision)
            {
                throw new ConcurrencyException();
            }

            if (head.CommitSequence < attempt.CommitSequence - 1)
            {
                throw new StorageException(); // beyond the end of the stream
            }

            if (head.StreamRevision < attempt.StreamRevision - attempt.Events.Count)
            {
                throw new StorageException(); // beyond the end of the stream
            }

            Logger.Debug(Resources.NoConflicts, attempt.StreamId);
            return Task.FromResult(true);
        }

        public override Task PostCommitAsync(ICommit committed, CancellationToken cancellationToken)
        {
            Track(committed);
            return Task.CompletedTask;
        }

        public override Task OnPurgeAsync(string bucketId, CancellationToken cancellationToken)
        {
            lock (_maxItemsToTrack)
            {
                if (bucketId == null)
                {
                    _heads.Clear();
                    _maxItemsToTrack.Clear();
                    return Task.CompletedTask;
                }

                var headsInBucket = _heads.Keys.Where(k => k.BucketId == bucketId).ToArray();
                foreach (var head in headsInBucket)
                {
                    RemoveHead(head);
                }

                return Task.CompletedTask;
            }
        }

        public override Task OnDeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            lock (_maxItemsToTrack)
            {
                RemoveHead(new HeadKey(bucketId, streamId));
                return Task.CompletedTask;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            _heads.Clear();
            _maxItemsToTrack.Clear();
        }

        public virtual void Track(ICommit committed)
        {
            if (committed == null)
            {
                return;
            }

            lock (_maxItemsToTrack)
            {
                UpdateStreamHead(committed);
                TrackUpToCapacity(committed);
            }
        }

        private void UpdateStreamHead(ICommit committed)
        {
            var headKey = GetHeadKey(committed);
            var head = GetStreamHead(headKey);
            if (AlreadyTracked(head))
            {
                _maxItemsToTrack.Remove(headKey);
            }

            head = head ?? committed;
            head = head.StreamRevision > committed.StreamRevision ? head : committed;

            _heads[headKey] = head;
        }

        private void RemoveHead(HeadKey head)
        {
            _heads.Remove(head);
            var node = _maxItemsToTrack.Find(head); // There should only be ever one or none
            if (node != null)
            {
                _maxItemsToTrack.Remove(node);
            }
        }

        private static bool AlreadyTracked(ICommit head)
        {
            return head != null;
        }

        private void TrackUpToCapacity(ICommit committed)
        {
            Logger.Verbose(Resources.TrackingCommit, committed.CommitSequence, committed.StreamId);
            _maxItemsToTrack.AddFirst(GetHeadKey(committed));
            if (_maxItemsToTrack.Count <= _maxStreamsToTrack)
            {
                return;
            }

            var expired = _maxItemsToTrack.Last.Value;
            Logger.Verbose(Resources.NoLongerTrackingStream, expired);

            _heads.Remove(expired);
            _maxItemsToTrack.RemoveLast();
        }

        public virtual bool Contains(ICommit attempt)
        {
            return GetStreamHead(GetHeadKey(attempt)) != null;
        }

        private ICommit GetStreamHead(HeadKey headKey)
        {
            lock (_maxItemsToTrack)
            {
                _heads.TryGetValue(headKey, out var head);
                return head;
            }
        }

        private static HeadKey GetHeadKey(ICommit commit)
        {
            return new HeadKey(commit.BucketId, commit.StreamId);
        }

        private static HeadKey GetHeadKey(CommitAttempt commitAttempt)
        {
            return new HeadKey(commitAttempt.BucketId, commitAttempt.StreamId);
        }

        private sealed class HeadKey : IEquatable<HeadKey>
        {
            public HeadKey(string bucketId, string streamId)
            {
                BucketId = bucketId;
                StreamId = streamId;
            }

            public string BucketId { get; }

            public string StreamId { get; }

            public bool Equals(HeadKey other)
            {
                if (ReferenceEquals(null, other))
                {
                    return false;
                }
                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                return string.Equals(BucketId, other.BucketId) &&
                       string.Equals(StreamId, other.StreamId);
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

                return obj is HeadKey key && Equals(key);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (BucketId.GetHashCode()*397) ^ StreamId.GetHashCode();
                }
            }
        }
    }
}