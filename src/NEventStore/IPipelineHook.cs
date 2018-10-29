using System;
using System.Threading;
using System.Threading.Tasks;

namespace NEventStore
{
    /// <summary>
    /// Provides the ability to hook into the pipeline of persisting a commit.
    /// </summary>
    /// <remarks>
    /// Instances of this class must be designed to be multi-thread safe such that they can be shared between threads.
    /// </remarks>
    public interface IPipelineHook : IDisposable
    {
        /// <summary>
        /// Hooks into the selection pipeline just prior to the commit being returned to the caller.
        /// </summary>
        /// <param name="committed">The commit to be filtered.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>If successful, returns a populated commit; otherwise returns null.</returns>
        Task<ICommit> SelectAsync(ICommit committed, CancellationToken cancellationToken);

        /// <summary>
        /// Hooks into the commit pipeline prior to persisting the commit to durable storage.
        /// </summary>
        /// <param name="attempt">The attempt to be committed.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>If processing should continue, returns true; otherwise returns false.</returns>
        Task<bool> PreCommitAsync(CommitAttempt attempt, CancellationToken cancellationToken);

        /// <summary>
        /// Hooks into the commit pipeline just after the commit has been *successfully* committed to durable storage.
        /// </summary>
        /// <param name="committed">The commit which has been persisted.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task PostCommitAsync(ICommit committed, CancellationToken cancellationToken);

        /// <summary>
        /// Invoked when a bucket has been purged. If buckedId is null, then all buckets have been purged.
        /// </summary>
        /// <param name="bucketId">The bucket Id that has been purged. Null when all buckets have been purged.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task OnPurgeAsync(string bucketId, CancellationToken cancellationToken);

        /// <summary>
        /// Invoked when a stream has been deleted.
        /// </summary>
        /// <param name="bucketId">The bucket Id from which the stream has been deleted.</param>
        /// <param name="streamId">The stream Id of the stream which has been deleted.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task OnDeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken);
    }
}