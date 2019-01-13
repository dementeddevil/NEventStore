namespace NEventStore.Diagnostics
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Threading.Tasks;
	using NEventStore.Persistence;

    public class PerformanceCounterPersistenceEngine : IPersistStreams
    {
        private readonly PerformanceCounters _counters;
        private readonly IPersistStreams _persistence;

        public PerformanceCounterPersistenceEngine(IPersistStreams persistence, string instanceName)
        {
            _persistence = persistence;
            _counters = new PerformanceCounters(instanceName);
        }

        public Task Initialize()
        {
            return _persistence.Initialize();
        }

        public async Task<ICommit> Commit(CommitAttempt attempt)
        {
            Stopwatch clock = Stopwatch.StartNew();
            ICommit commit = await _persistence.Commit(attempt);
            clock.Stop();
            _counters.CountCommit(attempt.Events.Count, clock.ElapsedMilliseconds);
            return commit;
        }

        public ICheckpoint ParseCheckpoint(string checkpointValue)
        {
            return LongCheckpoint.Parse(checkpointValue);
        }

        public Task<ICheckpoint> GetCheckpoint(string checkpointToken = null)
        {
            return _persistence.GetCheckpoint(checkpointToken);
        }

        public Task<IEnumerable<ICommit>> GetFromTo(string bucketId, DateTime start, DateTime end)
        {
            return _persistence.GetFromTo(bucketId, start, end);
        }

		public Task<IEnumerable<ICommit>> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            return _persistence.GetFrom(bucketId, streamId, minRevision, maxRevision);
        }

		public Task<IEnumerable<ICommit>> GetFrom(string bucketId, DateTime start)
        {
            return _persistence.GetFrom(bucketId, start);
        }

		public Task<IEnumerable<ICommit>> GetFrom(string checkpointToken)
        {
            return _persistence.GetFrom(checkpointToken);
        }

        public async Task<bool> AddSnapshot(ISnapshot snapshot)
        {
            bool result = await _persistence.AddSnapshot(snapshot);
            if (result)
            {
                _counters.CountSnapshot();
            }

            return result;
        }

        public Task<ISnapshot> GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            return _persistence.GetSnapshot(bucketId, streamId, maxRevision);
        }

        public virtual Task<IEnumerable<IStreamHead>> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        {
            return _persistence.GetStreamsToSnapshot(bucketId, maxThreshold);
        }

        public virtual Task Purge()
        {
            return _persistence.Purge();
        }

        public Task Purge(string bucketId)
        {
            return _persistence.Purge(bucketId);
        }

        public Task Drop()
        {
            return _persistence.Drop();
        }

        public Task DeleteStream(string bucketId, string streamId)
        {
            return _persistence.DeleteStream(bucketId, streamId);
        }

        public bool IsDisposed
        {
            get { return _persistence.IsDisposed; }
        }

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