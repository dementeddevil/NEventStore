using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Persistence;

namespace NEventStore
{
    public static class CommitEventsExtensions
    {
        /// <summary>
        /// Gets the corresponding commits from the stream indicated starting
        /// at the revision specified until the end of the stream sorted in
        /// ascending order--from oldest to newest.
        /// </summary>
        /// <param name="commitEvents">The <see cref="ICommitEvents"/> instance.</param>
        /// <param name="bucketId">The value which uniquely identifies bucket the stream belongs to.</param>
        /// <param name="streamId">The stream from which the events will be read.</param>
        /// <param name="minRevision">The minimum revision of the stream to be read.</param>
        /// <param name="maxRevision">The maximum revision of the stream to be read.</param>
        /// <returns>A series of committed events from the stream specified sorted in ascending order.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        public static Task<IEnumerable<ICommit>> GetFromAsync(this ICommitEvents commitEvents, string bucketId, string streamId, int minRevision, int maxRevision)
        {
            return commitEvents.GetFromAsync(bucketId, streamId, minRevision, maxRevision, CancellationToken.None);
        }

        /// <summary>
        /// Writes the to-be-committed events provided to the underlying
        /// persistence mechanism.
        /// </summary>
        /// <param name="commitEvents">The <see cref="ICommitEvents"/> instance.</param>
        /// <param name="attempt">The series of events and associated metadata to be committed.</param>
        /// <exception cref="ConcurrencyException" />
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        public static Task<ICommit> CommitAsync(this ICommitEvents commitEvents, CommitAttempt attempt)
        {
            return commitEvents.CommitAsync(attempt, CancellationToken.None);
        }

        /// <summary>
        /// Gets the corresponding commits from the stream indicated starting at the revision specified until the
        /// end of the stream sorted in ascending order--from oldest to newest from the default bucket.
        /// </summary>
        /// <param name="commitEvents">The <see cref="ICommitEvents"/> instance.</param>
        /// <param name="streamId">The stream from which the events will be read.</param>
        /// <param name="minRevision">The minimum revision of the stream to be read.</param>
        /// <param name="maxRevision">The maximum revision of the stream to be read.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A series of committed events from the stream specified sorted in ascending order.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        public static Task<IEnumerable<ICommit>> GetFromAsync(this ICommitEvents commitEvents, string streamId, int minRevision, int maxRevision, CancellationToken cancellationToken)
        {
            if (commitEvents == null)
            {
                throw new ArgumentNullException(nameof(commitEvents));
            }

            return commitEvents.GetFromAsync(Bucket.Default, streamId, minRevision, maxRevision, cancellationToken);
        }
    }
}