namespace NEventStore.Example
{
    using System;
	using System.Threading.Tasks;

    public class AuthorizationPipelineHook : PipelineHookBase
    {
        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public override Task<ICommit> Select(ICommit committed)
        {
            // return null if the user isn't authorized to see this commit
            return Task.FromResult(committed);
        }

        public override Task<bool> PreCommit(CommitAttempt attempt)
        {
            // Can easily do logging or other such activities here
            return Task.FromResult(true); // true == allow commit to continue, false = stop.
        }

        public override Task PostCommit(ICommit committed)
        {
            // anything to do after the commit has been persisted.
			return Task.FromResult(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            // no op
        }
    }
}