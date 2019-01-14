using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Logging;

namespace NEventStore.Persistence
{
    public class PipelineHooksAwarePersistanceDecorator : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(PipelineHooksAwarePersistanceDecorator));
        private readonly IPersistStreams _original;
        private readonly IEnumerable<IPipelineHook> _pipelineHooks;

        public PipelineHooksAwarePersistanceDecorator(IPersistStreams original, IEnumerable<IPipelineHook> pipelineHooks)
        {
            _original = original ?? throw new ArgumentNullException(nameof(original));
            _pipelineHooks = pipelineHooks ?? throw new ArgumentNullException(nameof(pipelineHooks));
        }

        public void Dispose()
        {
            _original.Dispose();
        }

        public Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            return _original.GetFromAsync(bucketId, streamId, minRevision, maxRevision, cancellationToken);
        }

        public Task<ICommit> CommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            return _original.CommitAsync(attempt, cancellationToken);
        }

        public Task<ISnapshot> GetSnapshotAsync(string bucketId, string streamId, int maxRevision, CancellationToken cancellationToken)
        {
            return _original.GetSnapshotAsync(bucketId, streamId, maxRevision, cancellationToken);
        }

        public Task<bool> AddSnapshotAsync(ISnapshot snapshot, CancellationToken cancellationToken)
        {
            return _original.AddSnapshotAsync(snapshot, cancellationToken);
        }

        public Task<IEnumerable<IStreamHead>> GetStreamsToSnapshotAsync(string bucketId, int maxThreshold, CancellationToken cancellationToken)
        {
            return _original.GetStreamsToSnapshotAsync(bucketId, maxThreshold, cancellationToken);
        }

        public Task InitializeAsync(CancellationToken cancellationToken)
        {
            return _original.InitializeAsync(cancellationToken);
        }

        public async Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, DateTime start, CancellationToken cancellationToken)
        {
            return await ExecuteHooks(
                await _original
                    .GetFromAsync(bucketId, start, cancellationToken)
                    .ConfigureAwait(false),
                cancellationToken).ConfigureAwait(false);
        }

        public async Task<IEnumerable<ICommit>> GetFromAsync(Int64 checkpointToken, CancellationToken cancellationToken)
        {
            return await ExecuteHooks(
                await _original
                    .GetFromAsync(checkpointToken, cancellationToken)
                    .ConfigureAwait(false),
                cancellationToken).ConfigureAwait(false);
        }

        public async Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, Int64 checkpointToken, CancellationToken cancellationToken)
        {
            return await ExecuteHooks(
                await _original
                    .GetFromAsync(bucketId, checkpointToken, cancellationToken)
                    .ConfigureAwait(false),
                cancellationToken).ConfigureAwait(false);
        }

        public async Task<IEnumerable<ICommit>> GetFromToAsync(string bucketId, DateTime start, DateTime end, CancellationToken cancellationToken)
        {
            return await ExecuteHooks(
                await _original
                    .GetFromToAsync(bucketId, start, end, cancellationToken)
                    .ConfigureAwait(false),
                cancellationToken).ConfigureAwait(false);
        }

        public async Task PurgeAsync(CancellationToken cancellationToken)
        {
            await _original.PurgeAsync(cancellationToken).ConfigureAwait(false);
            foreach (var pipelineHook in _pipelineHooks)
            {
                await pipelineHook
                    .OnPurgeAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        public async Task PurgeAsync(string bucketId, CancellationToken cancellationToken)
        {
            await _original.PurgeAsync(bucketId, cancellationToken).ConfigureAwait(false);
            foreach (var pipelineHook in _pipelineHooks)
            {
                await pipelineHook.OnPurgeAsync(bucketId, cancellationToken).ConfigureAwait(false);
            }
        }

        public Task DropAsync(CancellationToken cancellationToken)
        {
            return _original.DropAsync(cancellationToken);
        }

        public async Task DeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            await _original
                .DeleteStreamAsync(bucketId, streamId, cancellationToken)
                .ConfigureAwait(false);

            foreach (var pipelineHook in _pipelineHooks)
            {
                await pipelineHook
                    .OnDeleteStreamAsync(bucketId, streamId, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        public bool IsDisposed => _original.IsDisposed;

        private async Task<IEnumerable<ICommit>> ExecuteHooks(IEnumerable<ICommit> commits, CancellationToken cancellationToken)
        {
            var result = new List<ICommit>();
            foreach (var commit in commits)
            {
                var filtered = commit;
                foreach (var hook in _pipelineHooks)
                {
                    filtered = await hook
                        .SelectAsync(filtered, cancellationToken)
                        .ConfigureAwait(false);

                    if (filtered == null)
                    {
                        Logger.Info(Resources.PipelineHookSkippedCommit, hook.GetType(), commit.CommitId);
                        break;
                    }
                }

                if (filtered == null)
                {
                    Logger.Info(Resources.PipelineHookFilteredCommit);
                }
                else
                {
                    result.Add(filtered);
                }
            }

            return result;
        }
    }
}