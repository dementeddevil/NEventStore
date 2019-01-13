using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Logging;
using NEventStore.Persistence;

namespace NEventStore
{
    public class OptimisticEventStore : IStoreEvents, ICommitEvents
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(OptimisticEventStore));
        private readonly IPersistStreams _persistence;
        private readonly IEnumerable<IPipelineHook> _pipelineHooks;

        public OptimisticEventStore(IPersistStreams persistence, IEnumerable<IPipelineHook> pipelineHooks)
        {
            if (persistence == null)
            {
                throw new ArgumentNullException(nameof(persistence));
            }

            _pipelineHooks = pipelineHooks ?? new IPipelineHook[0];
            _persistence = new PipelineHooksAwarePersistanceDecorator(persistence, _pipelineHooks);
        }

        public virtual Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            //var result = new List<ICommit>();
            //foreach (var commit in await _persistence
            //    .GetFromAsync(bucketId, streamId, minRevision, maxRevision, cancellationToken)
            //    .ConfigureAwait(false))
            //{
            //    var filtered = commit;
            //    foreach (var hook in _pipelineHooks)
            //    {
            //        cancellationToken.ThrowIfCancellationRequested();

            //        filtered = await hook
            //            .SelectAsync(filtered, cancellationToken)
            //            .ConfigureAwait(false);

            //        if (filtered == null)
            //        {
            //            Logger.Info(Resources.PipelineHookSkippedCommit, hook.GetType(), commit.CommitId);
            //            break;
            //        }
            //    }

            //    cancellationToken.ThrowIfCancellationRequested();

            //    if (filtered == null)
            //    {
            //        Logger.Info(Resources.PipelineHookFilteredCommit);
            //    }
            //    else
            //    {
            //        result.Add(filtered);
            //    }
            //}

            //return result;
            return _persistence.GetFromAsync(bucketId, streamId, minRevision, maxRevision, cancellationToken);
        }

        public virtual async Task<ICommit> CommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            Guard.NotNull(() => attempt, attempt);

            foreach (var hook in _pipelineHooks)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Logger.Debug(Resources.InvokingPreCommitHooks, attempt.CommitId, hook.GetType());
                if (await hook.PreCommitAsync(attempt, cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                Logger.Info(Resources.CommitRejectedByPipelineHook, hook.GetType(), attempt.CommitId);
                return null;
            }

            // Last chance before we attempt to commit
            cancellationToken.ThrowIfCancellationRequested();

            Logger.Info(Resources.CommittingAttempt, attempt.CommitId, attempt.Events.Count);
            var commit = await _persistence.CommitAsync(attempt, cancellationToken).ConfigureAwait(false);

            foreach (var hook in _pipelineHooks)
            {
                Logger.Debug(Resources.InvokingPostCommitPipelineHooks, attempt.CommitId, hook.GetType());
                await hook.PostCommitAsync(commit, cancellationToken).ConfigureAwait(false);
            }

            return commit;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual Task<IEventStream> CreateStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            Logger.Info(Resources.CreatingStream, streamId, bucketId);
            return Task.FromResult<IEventStream>(new OptimisticEventStream(bucketId, streamId, this));
        }

        public virtual Task<IEventStream> OpenStreamAsync(string bucketId, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            maxRevision = maxRevision <= 0 ? int.MaxValue : maxRevision;

            Logger.Debug(Resources.OpeningStreamAtRevision, streamId, bucketId, minRevision, maxRevision);
            return Task.FromResult<IEventStream>(new OptimisticEventStream(bucketId, streamId, this, minRevision, maxRevision, cancellationToken));
        }

        public virtual Task<IEventStream> OpenStreamAsync(ISnapshot snapshot, int maxRevision, CancellationToken cancellationToken)
        {
            if (snapshot == null)
            {
                throw new ArgumentNullException(nameof(snapshot));
            }

            Logger.Debug(Resources.OpeningStreamWithSnapshot, snapshot.StreamId, snapshot.StreamRevision, maxRevision);
            maxRevision = maxRevision <= 0 ? int.MaxValue : maxRevision;
            return Task.FromResult<IEventStream>(new OptimisticEventStream(snapshot, this, maxRevision, cancellationToken));
        }

        public virtual IPersistStreams Advanced => _persistence;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            Logger.Info(Resources.ShuttingDownStore);
            _persistence.Dispose();
            foreach (var hook in _pipelineHooks)
            {
                hook.Dispose();
            }
        }
    }
}