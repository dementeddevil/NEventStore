namespace NEventStore
{
    using System.Threading;
    using System.Threading.Tasks;

    public abstract class PipelineHookBase : IPipelineHook
    {
        public virtual void Dispose()
        {}

        public virtual Task<ICommit> SelectAsync(ICommit committed, CancellationToken cancellationToken)
        {
            return Task.FromResult(committed);
        }

        public virtual Task<bool> PreCommitAsync(CommitAttempt attempt, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        public virtual Task PostCommitAsync(ICommit committed, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public virtual Task OnPurgeAsync(string bucketId, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public virtual Task OnDeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}