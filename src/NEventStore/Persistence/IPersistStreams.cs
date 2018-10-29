namespace NEventStore.Persistence
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Indicates the ability to adapt the underlying persistence infrastructure to behave like a stream of events.
    /// </summary>
    /// <remarks>
    ///     Instances of this class must be designed to be multi-thread safe such that they can be shared between threads.
    /// </remarks>
    public interface IPersistStreams : IDisposable, ICommitEvents, IAccessSnapshots
    {
        /// <summary>
        ///     Gets a value indicating whether this instance has been disposed of.
        /// </summary>
        bool IsDisposed { get; }

        /// <summary>
        ///     Initializes and prepares the storage for use, if not already performed.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        Task InitializeAsync(CancellationToken cancellationToken);

        /// <summary>
        ///     Gets all commits on or after from the specified starting time.
        /// </summary>
        /// <param name="bucketId">The value which uniquely identifies bucket the stream belongs to.</param>
        /// <param name="start">The point in time at which to start.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>All commits that have occurred on or after the specified starting time.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        Task<IEnumerable<ICommit>> GetFromAsync(string bucketId, DateTime start, CancellationToken cancellationToken);

        /// <summary>
        ///     Gets all commits after from the specified checkpoint. Use null to get from the beginning.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="checkpointToken">The checkpoint token.</param>
        /// <returns>An enumerable of Commits.</returns>
        Task<IEnumerable<ICommit>> GetFromAsync(CancellationToken cancellationToken, string checkpointToken = null);

        /// <summary>
        /// Gets a checkpoint object that is comparable with other checkpoints from this storage engine.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="checkpointToken">The checkpoint token.</param>
        /// <returns>A <see cref="ICheckpoint"/> instance.</returns>
        Task<ICheckpoint> GetCheckpointAsync(CancellationToken cancellationToken, string checkpointToken = null);

        /// <summary>
        ///     Gets all commits on or after from the specified starting time and before the specified end time.
        /// </summary>
        /// <param name="bucketId">The value which uniquely identifies bucket the stream belongs to.</param>
        /// <param name="start">The point in time at which to start.</param>
        /// <param name="end">The point in time at which to end.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>All commits that have occurred on or after the specified starting time and before the end time.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        Task<IEnumerable<ICommit>> GetFromToAsync(string bucketId, DateTime start, DateTime end, CancellationToken cancellationToken);

        /// <summary>
        ///     Gets a set of commits that has not yet been dispatched.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The set of commits to be dispatched.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        Task<IEnumerable<ICommit>> GetUndispatchedCommitsAsync(CancellationToken cancellationToken);

        /// <summary>
        ///     Marks the commit specified as dispatched.
        /// </summary>
        /// <param name="commit">The commit to be marked as dispatched.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        Task MarkCommitAsDispatchedAsync(ICommit commit, CancellationToken cancellationToken);

        /// <summary>
        /// Completely DESTROYS the contents of ANY and ALL streams that have
        /// been successfully persisted.  Use with caution.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task PurgeAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Completely DESTROYS the contents of ANY and ALL streams that have
        /// been successfully persisted in the specified bucket.  Use with caution.
        /// </summary>
        /// <param name="bucketId">The bucket Id from which the stream is to be deleted.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task PurgeAsync(string bucketId, CancellationToken cancellationToken);

        /// <summary>
        /// Completely DESTROYS the contents and schema (if applicable)
        /// containing ANY and ALL streams that have been successfully
        /// persisted in the specified bucket.  Use with caution.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task DropAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Deletes a stream.
        /// </summary>
        /// <param name="bucketId">The bucket Id from which the stream is to be deleted.</param>
        /// <param name="streamId">The stream Id of the stream that is to be deleted.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task DeleteStreamAsync(string bucketId, string streamId, CancellationToken cancellationToken);
    }
}