using System.Threading.Tasks;
namespace NEventStore
{
    public abstract class PipelineHookBase : IPipelineHook
    {
        public virtual void Dispose()
        {}

        public virtual Task<ICommit> Select(ICommit committed)
        {
            return Task.FromResult(committed);
        }

        public virtual Task<bool> PreCommit(CommitAttempt attempt)
        {
            return Task.FromResult(true);
        }

        public virtual Task PostCommit(ICommit committed)
		{
			return Task.FromResult(true);
		}

        public virtual Task OnPurge(string bucketId)
		{
			return Task.FromResult(true);
		}

        public virtual Task OnDeleteStream(string bucketId, string streamId)
		{
			return Task.FromResult(true);
		}
	}
}