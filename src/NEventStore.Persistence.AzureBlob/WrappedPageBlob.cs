using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NEventStore.Logging;
using AzureStorage = Microsoft.WindowsAzure.Storage;

namespace NEventStore.Persistence.AzureBlob
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
	/// A WrappedPageBlob
	/// </summary>
	public class WrappedPageBlob
	{
		private const int BlobPageSize = 512;

		private readonly CloudPageBlob _pageBlob;
		private static readonly ILog Logger = LogFactory.BuildLogger(typeof(WrappedPageBlob));

		#region construction

		/// <summary>
		/// Create a new wrapped page blob
		/// </summary>
		/// <param name="pageBlob"></param>
		private WrappedPageBlob(CloudPageBlob pageBlob)
		{
            _pageBlob = pageBlob ?? throw new ArgumentNullException(nameof(pageBlob));
		}

        /// <summary>
        /// Creates a new page blob if it does not already exist.
        /// </summary>
        /// <param name="blobContainer">the container that owns the blob</param>
        /// <param name="blobId">the id of the blob</param>
        /// <param name="startingPages">default number of pages to start with</param>
        /// <param name="cancellationToken"></param>
        /// <returns>the already existing or newly created page blob</returns>
        /// <remarks>This call should only be used when uncertain if the blob already exists.  It costs an extra API call</remarks>
        public static async Task<WrappedPageBlob> CreateNewIfNotExistsAsync(CloudBlobContainer blobContainer, string blobId, int startingPages, CancellationToken cancellationToken)
		{
			var pageBlob = await GetAssumingExistsAsync(blobContainer, blobId, cancellationToken).ConfigureAwait(false);
			return pageBlob ?? await CreateNewAsync(blobContainer, blobId, startingPages, cancellationToken).ConfigureAwait(false);
		}

		/// <summary>
		/// Gets all wrapped page blobs matching the blob id prefix
		/// </summary>
		/// <param name="blobContainer"></param>
		/// <param name="blobId"></param>
		/// <returns></returns>
		public static IEnumerable<WrappedPageBlob> GetAllMatchingPrefix(CloudBlobContainer blobContainer, string prefix)
		{
			Logger.Verbose("Getting all blobs with prefix [{0}]", prefix);

			var pageBlobs = blobContainer
				.ListBlobs(prefix, true, BlobListingDetails.Metadata).OfType<CloudPageBlob>();

			foreach (var pageBlob in pageBlobs)
			{ yield return new WrappedPageBlob(pageBlob); }
		}

        /// <summary>
        /// Gets a wrapped page blob.
        /// </summary>
        /// <param name="blobContainer">the container that owns the blob</param>
        /// <param name="blobId">the id of the blob</param>
        /// <param name="startingPages">default number of pages to start with</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<WrappedPageBlob> GetAssumingExistsAsync(CloudBlobContainer blobContainer, string blobId, CancellationToken cancellationToken)
		{
			Logger.Verbose("Getting blob with id [{0}]", blobId);
            var pageBlobs = await blobContainer
                .ListBlobsSegmentedAsync(
                    blobId,
                    true,
                    BlobListingDetails.Metadata,
                    null,
                    null,
                    null,
                    null,
                    cancellationToken)
                .ConfigureAwait(false);
            var pageBlob = pageBlobs
                .Results
                .OfType<CloudPageBlob>()
                .SingleOrDefault();

			return (pageBlob == null) ? null : new WrappedPageBlob(pageBlob);
		}

        /// <summary>
        /// Creates a new wrapped page blob.  If it exists is will throw, this call assumes it does not exist
        /// </summary>
        /// <param name="blobContainer">the container that owns the blob</param>
        /// <param name="blobId">the id of the blob</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<WrappedPageBlob> CreateNewAsync(CloudBlobContainer blobContainer, string blobId, int startingPages, CancellationToken cancellationToken)
		{
			Logger.Verbose("Creating new blob with id [{0}]", blobId);

			var pageBlob = blobContainer.GetPageBlobReference(blobId);
			await pageBlob
                .CreateAsync((long)512 * startingPages, cancellationToken)
                .ConfigureAwait(false);

			await pageBlob
                .FetchAttributesAsync(cancellationToken)
                .ConfigureAwait(false);

			return new WrappedPageBlob(pageBlob);
		}

		#endregion

		#region casts

		/// <summary>
		/// Implicit cast to get to the page blob
		/// </summary>
		/// <param name="wrapped"></param>
		/// <returns></returns>
		public static implicit operator CloudPageBlob(WrappedPageBlob wrapped)
		{ return wrapped._pageBlob; }

		#endregion

		/// <summary>
		/// Gets the actual page blob metadata.
		/// </summary>
		/// <remarks>Anything added to this dictionary will be submitted during a call to set metadata</remarks>
		public IDictionary<string, string> Metadata => _pageBlob.Metadata;

        /// <summary>
		/// Gets the actual properties of the page blob
		/// </summary>
		public BlobProperties Properties => _pageBlob.Properties;

        /// <summary>
		/// Get the actual name of the page blob
		/// </summary>
		public string Name => _pageBlob.Name;

        /// <summary>
        /// Re-fetches the blob attributes.  this only needs to be done when fresher attributes
        /// than fetched when the wrapped page was first created
        /// </summary>
        /// <param name="accessCondition">the access condition</param>
        /// <param name="cancellationToken"></param>
        public async Task RefetchAttributesAsync(bool disregardConcurrency, CancellationToken cancellationToken)
		{
            try
            {
                var accessCondition = disregardConcurrency
                    ? null
                    : AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag);

                Logger.Verbose("Fetching attributes for blob [{0}]", _pageBlob.Uri);
                await _pageBlob
                    .FetchAttributesAsync(accessCondition, null, null, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (AzureStorage.StorageException ex)
            {
                throw HandleAndRemapCommonExceptions(ex);
            }
		}

        /// <summary>
        /// Sets the metadata that is currently set
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task SetMetadataAsync(CancellationToken cancellationToken)
		{
			Logger.Verbose("Setting metadata for blob [{0}], etag [{1}]", _pageBlob.Uri, _pageBlob.Properties.ETag);

            try
            {
                await _pageBlob
                    .SetMetadataAsync(
                        AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag),
                        null,
                        null,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (AzureStorage.StorageException ex)
            {
                throw HandleAndRemapCommonExceptions(ex);
            }
		}

        /// <summary>
        /// Download the page range specified
        /// </summary>
        /// <param name="startIndex">start index</param>
        /// <param name="endIndex">end index</param>
        /// <param name="disregardConcurrency"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<byte[]> DownloadBytesAsync(
            int startIndex,
            int endIndex,
            bool disregardConcurrency,
            CancellationToken cancellationToken)
		{
            try
            {
                var accessCondition = disregardConcurrency
                    ? null
                    : AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag);

                var data = new byte[endIndex - startIndex];
                Logger.Verbose("Downloading [{0}] bytes for blob [{1}], etag [{2}]", data.Length, _pageBlob.Uri,
                    _pageBlob.Properties.ETag);

                await _pageBlob
                    .DownloadRangeToByteArrayAsync(
                        data,
                        0,
                        startIndex,
                        data.Length,
                        accessCondition,
                        null,
                        null,
                        cancellationToken)
                    .ConfigureAwait(false);
                return data;
            }
            catch (AzureStorage.StorageException ex)
            {
                throw HandleAndRemapCommonExceptions(ex);
            }
		}

        /// <summary>
        /// Writes to the page blob
        /// </summary>
        /// <param name="pageDataWithHeaderAligned">data to write, aligned with the header appended to it.</param>
        /// <param name="startOffsetAligned">where writing will start (aligned)</param>
        /// <param name="currentHeaderDefinition">non aligned offset where the new header will be written</param>
        /// <param name="newHeaderOffsetBytesNonAligned">start index for where the new header will be written (not aligned)</param>
        /// <param name="cancellationToken"></param>
        internal async Task WriteAsync(
            Stream pageDataWithHeaderAligned,
            int startOffsetAligned,
			int newHeaderOffsetBytesNonAligned,
            HeaderDefinitionMetadata currentHeaderDefinition,
            CancellationToken cancellationToken)
		{
            try
            {
                Logger.Verbose("Writing [{0}] bytes for blob [{1}], etag [{2}]", pageDataWithHeaderAligned.Length,
                    _pageBlob.Uri, _pageBlob.Properties.ETag);

                // If our entire payload is less than four megabytes we can write this operation in a single commit.
                // otherwise we must chunk requiring for some more complex managment of our header data
                const int maxSingleWriteSizeBytes = 1024 * 1024 * 4;
                if (pageDataWithHeaderAligned.Length <= maxSingleWriteSizeBytes)
                {
                    await _pageBlob
                        .WritePagesAsync(
                            pageDataWithHeaderAligned,
                            startOffsetAligned,
                            null,
                            AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag),
                            null,
                            null,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    // if there is no header yet, then we have nothing to do around saving off the old header.
                    if (currentHeaderDefinition.HeaderSizeInBytes != 0)
                    {
                        // the first thing we must do is copy the old header to the new assumed location.
                        var seralizedHeader = await this
                            .DownloadBytesAsync(
                                currentHeaderDefinition.HeaderStartLocationOffsetBytes,
                                currentHeaderDefinition.HeaderStartLocationOffsetBytes +
                                currentHeaderDefinition.HeaderSizeInBytes,
                                false,
                                cancellationToken)
                            .ConfigureAwait(false);

                        // get the start location where we will write the header.  must be page aligned
                        var emptyFirstBytesCount = newHeaderOffsetBytesNonAligned % 512;
                        var headerAlignedStartOffsetBytes = newHeaderOffsetBytesNonAligned - emptyFirstBytesCount;
                        var alignedBytesRequired =
                            GetPageAlignedSize(emptyFirstBytesCount + currentHeaderDefinition.HeaderSizeInBytes);
                        var alignedSerializedHeader = new byte[alignedBytesRequired];
                        Array.Copy(seralizedHeader, 0, alignedSerializedHeader, emptyFirstBytesCount,
                            seralizedHeader.Length);
                        using (var temp = new MemoryStream(alignedSerializedHeader, false))
                        {
                            await _pageBlob
                                .WritePagesAsync(
                                    temp,
                                    headerAlignedStartOffsetBytes,
                                    null,
                                    AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag),
                                    null,
                                    null,
                                    cancellationToken)
                                .ConfigureAwait(false);
                        }
                    }

                    var allocatedFourMegs = new byte[maxSingleWriteSizeBytes];
                    var currentOffset = startOffsetAligned;

                    // our last write must be at least newHeaderSize in size otherwise we run a risk of having a partial header
                    var newHeaderSize = startOffsetAligned + pageDataWithHeaderAligned.Length -
                                        newHeaderOffsetBytesNonAligned;
                    var remainingBytesToWrite = pageDataWithHeaderAligned.Length;
                    while (remainingBytesToWrite != 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var amountToWrite = maxSingleWriteSizeBytes;
                        var potentialRemaining = remainingBytesToWrite - amountToWrite;
                        if (potentialRemaining < 0)
                        {
                            potentialRemaining = remainingBytesToWrite;
                        }

                        if (potentialRemaining < newHeaderSize)
                        {
                            var howMuchLessWeNeedToWriteAligned = (int) (newHeaderSize - potentialRemaining);
                            howMuchLessWeNeedToWriteAligned = GetPageAlignedSize(howMuchLessWeNeedToWriteAligned);
                            amountToWrite -= howMuchLessWeNeedToWriteAligned;
                        }

                        var lastAmountRead = await pageDataWithHeaderAligned
                            .ReadAsync(
                                allocatedFourMegs,
                                0,
                                amountToWrite,
                                cancellationToken)
                            .ConfigureAwait(false);

                        remainingBytesToWrite -= lastAmountRead;
                        using (var tempStream = new MemoryStream(allocatedFourMegs, 0, lastAmountRead, false, false))
                        {
                            await _pageBlob
                                .WritePagesAsync(
                                    tempStream,
                                    currentOffset,
                                    null,
                                    AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag),
                                    null,
                                    null,
                                    cancellationToken)
                                .ConfigureAwait(false);
                        }

                        currentOffset += lastAmountRead;
                    }
                }

                Logger.Verbose("Wrote [{0}] bytes for blob [{1}], etag [{2}]", pageDataWithHeaderAligned.Length,
                    _pageBlob.Uri, _pageBlob.Properties.ETag);
            }
            catch (AzureStorage.StorageException ex)
            {
                throw HandleAndRemapCommonExceptions(ex);
            }
		}

        /// <summary>
        /// Resized the blob
        /// </summary>
        /// <param name="neededSize"></param>
        /// <param name="cancellationToken"></param>
        public async Task ResizeAsync(int neededSize, CancellationToken cancellationToken)
		{
			Logger.Verbose("Resizing page blob [{0}], etag [{1}]", _pageBlob.Uri, _pageBlob.Properties.ETag);

            try
            {
                // we are going to grow by 50%
                var newSize = (int) Math.Floor(neededSize * 1.5);
                newSize = GetPageAlignedSize(newSize);

                await _pageBlob
                    .ResizeAsync(
                        newSize,
                        AccessCondition.GenerateIfMatchCondition(_pageBlob.Properties.ETag),
                        null,
                        null,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Microsoft.WindowsAzure.Storage.StorageException ex)
            {
                throw HandleAndRemapCommonExceptions(ex);
            }
		}

		/// <summary>
		/// Get page aligned number of bytes from a non page aligned number
		/// </summary>
		/// <param name="nonAligned"></param>
		/// <returns></returns>
		private int GetPageAlignedSize(int nonAligned)
		{
			var remainder = nonAligned % BlobPageSize;
			return (remainder == 0) ? nonAligned : nonAligned + (BlobPageSize - remainder);
		}

		/// <summary>
		/// Helper to remat exceptions
		/// </summary>
		/// <param name="ex"></param>
		/// <returns></returns>
		private static Exception HandleAndRemapCommonExceptions(Microsoft.WindowsAzure.Storage.StorageException ex)
		{
			if (ex.Message.Contains("412"))
			{ return new ConcurrencyException("concurrency detected.  see inner exception for details", ex); }
			else
			{ return ex; }
		}
	}
}
