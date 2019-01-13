    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

namespace NEventStore.Persistence
{
    public static class PersistStreamsExtensions
    {
        /// <summary>
        /// Gets all commits on or after from the specified starting time from the default bucket.
        /// </summary>
        /// <param name="persistStreams">The IPersistStreams instance.</param>
        /// <param name="start">The point in time at which to start.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>All commits that have occurred on or after the specified starting time.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        public static Task<IEnumerable<ICommit>> GetFromAsync(this IPersistStreams persistStreams, DateTime start, CancellationToken cancellationToken)
        {
            if (persistStreams == null)
            {
                throw new ArgumentNullException(nameof(persistStreams));
            }

            return persistStreams.GetFromAsync(Bucket.Default, start, cancellationToken);
        }

        /// <summary>
        /// Gets all commits on or after from the specified starting time and before the specified end time from the default bucket.
        /// </summary>
        /// <param name="persistStreams">The IPersistStreams instance.</param>
        /// <param name="start">The point in time at which to start.</param>
        /// <param name="end">The point in time at which to end.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>All commits that have occurred on or after the specified starting time and before the end time.</returns>
        /// <exception cref="StorageException" />
        /// <exception cref="StorageUnavailableException" />
        public static Task<IEnumerable<ICommit>> GetFromToAsync(this IPersistStreams persistStreams, DateTime start, DateTime end, CancellationToken cancellationToken)
        {
            if (persistStreams == null)
            {
                throw new ArgumentNullException(nameof(persistStreams));
            }

            return persistStreams.GetFromToAsync(Bucket.Default, start, end, cancellationToken);
        }

        /// <summary>
        /// Deletes a stream from the default bucket.
        /// </summary>
        /// <param name="persistStreams">The IPersistStreams instance.</param>
        /// <param name="streamId">The stream id to be deleted.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public static Task DeleteStreamAsync(this IPersistStreams persistStreams, string streamId, CancellationToken cancellationToken)
        {
            if (persistStreams == null)
            {
                throw new ArgumentNullException(nameof(persistStreams));
            }

            return persistStreams.DeleteStreamAsync(Bucket.Default, streamId, cancellationToken);
        }

        /// <summary>
        /// Gets all commits after from start checkpoint.
        /// </summary>
        /// <param name="persistStreams">The IPersistStreams instance.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public static Task<IEnumerable<ICommit>> GetFromStartAsync(this IPersistStreams persistStreams, CancellationToken cancellationToken)
        {
            if (persistStreams == null)
            {
                throw new ArgumentNullException(nameof(persistStreams));
            }

            return persistStreams.GetFromAsync(0, cancellationToken);
        }
    }
}