using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NEventStore.Logging;
using NEventStore.Serialization;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// Main engine for using Azure blob storage for event sourcing.
	/// The general pattern is that all commits for a given stream live within a single page blob.
	/// As new commits are added, they are placed at the end of the page blob.
	/// </summary>
	public class AzureBlobPersistenceEngine : IPersistStreams
	{
		private static readonly ILog Logger = LogFactory.BuildLogger(typeof(AzureBlobPersistenceEngine));
		private static int _connectionLimitSet;

		private const string _eventSourcePrefix = "evsrc";
		private const string _rootContainerName = "$root";
		private const string _checkpointBlobName = "checkpoint";
		private const string _checkpointNumberKey = "checkpointnumber";
		private const string _hasUndispatchedCommitsKey = "hasUndispatchedCommits";
		private const string _isEventStreamAggregateKey = "isEventStreamAggregate";

		private const int _blobPageSize = 512;
		private const int _headerDefinitionMetadataPages = (HeaderDefinitionMetadata.RawSize / _blobPageSize) + 1;
		private const int _headerDefinitionMetadataBytes = _headerDefinitionMetadataPages * _blobPageSize;
		private static byte[] _maxFillSpace = new byte[1024];

		private readonly ISerialize _serializer;
		private readonly AzureBlobPersistenceOptions _options;
		private readonly CloudBlobContainer _blobContainer;
		private readonly CloudBlobClient _blobClient;
		private int _initialized;
		private bool _disposed;

		/// <summary>
		/// Create a new engine.
		/// </summary>
		/// <param name="connectionString">The Azure blob storage connection string.</param>
		/// <param name="serializer">The serializer to use.</param>
		/// <param name="options">Options for the Azure blob storage.</param>
		public AzureBlobPersistenceEngine(string connectionString, ISerialize serializer, AzureBlobPersistenceOptions options = null)
		{
			if (String.IsNullOrEmpty(connectionString))
			{ throw new ArgumentException("connectionString cannot be null or empty"); }
			if (serializer == null)
			{ throw new ArgumentNullException("serializer"); }
			if (options == null)
			{ throw new ArgumentNullException("options"); }

			_serializer = serializer;
			_options = options;

			var storageAccount = CloudStorageAccount.Parse(connectionString);
			_blobClient = storageAccount.CreateCloudBlobClient();
			_blobContainer = _blobClient.GetContainerReference(GetContainerName());
		}

		/// <summary>
		/// Is the engine disposed?
		/// </summary>
		public bool IsDisposed
		{ get { return _disposed; } }

		/// <summary>
		/// Connect to Azure storage and get a reference to the container object,
		/// creating it if it does not exist.
		/// </summary>
		public void Initialize()
		{
			// we want to increase the connection limit used to communicate with via HTTP rest
			// calls otherwise we will feel significant performance degradation.  This can also
			// be modified via configuration but that would be a very leaky concern to the
			// application developer.
			if (Interlocked.Increment(ref _connectionLimitSet) < 2)
			{
				Uri uri = new Uri(_blobContainer.Uri.AbsoluteUri);
				var sp = ServicePointManager.FindServicePoint(uri);
				sp.ConnectionLimit = _options.ParallelConnectionLimit;

				// make sure we have a checkpoint aggregate
				var blobContainer = _blobClient.GetContainerReference(_rootContainerName);
				blobContainer.CreateIfNotExists();

				var pageBlobReference = blobContainer.GetPageBlobReference(_checkpointBlobName);
				try
				{ pageBlobReference.Create(512, accessCondition: AccessCondition.GenerateIfNoneMatchCondition("*")); }
				catch (Microsoft.WindowsAzure.Storage.StorageException ex)
				{
					// 409 means it was already there
					if (!ex.Message.Contains("409"))
					{ throw; }
				}
			}

			if (Interlocked.Increment(ref _initialized) < 2)
			{ _blobContainer.CreateIfNotExists(); }
		}

		/// <summary>
		/// Not Implemented.
		/// </summary>
		/// <param name="checkpointToken"></param>
		/// <returns></returns>
		public ICheckpoint GetCheckpoint(string checkpointToken = null)
		{ throw new NotImplementedException(); }

		/// <summary>
		/// Gets the list of commits from a given blobEntry, starting from a given date
		/// until the present.
		/// </summary>
		/// <param name="bucketId">The blobEntry id to pull commits from.</param>
		/// <param name="start">The starting date for commits.</param>
		/// <returns>The list of commits from the given blobEntry and greater than or equal to the start date.</returns>
		public IEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
		{ return GetFromTo(bucketId, start, DateTime.MaxValue); }

		/// <summary>
		/// Not Implemented.
		/// </summary>
		/// <param name="checkpointToken"></param>
		/// <returns></returns>
		public IEnumerable<ICommit> GetFrom(string checkpointToken = null)
		{
			var containers = _blobClient.ListContainers(_eventSourcePrefix);
			var allCommitDefinitions = new List<Tuple<CloudPageBlob, PageBlobCommitDefinition>>();

			foreach (var container in containers)
			{
				var blobs = container.ListBlobs(GetContainerName(), true, BlobListingDetails.Metadata).OfType<CloudPageBlob>();

				// this could be a tremendous amount of data.  Depending on system used
				// this may not be performant enough and may require some sort of index be built.
				foreach (var pageBlob in blobs)
				{
					var header = GetHeader(pageBlob, null);
					foreach (var definition in header.PageBlobCommitDefinitions)
					{ allCommitDefinitions.Add(new Tuple<CloudPageBlob, PageBlobCommitDefinition>(pageBlob, definition)); }
				}
			}

			// now sort the definitions so we can return out sorted
			var orderedCommitDefinitions = allCommitDefinitions.OrderBy((x) => x.Item2.Checkpoint);
			foreach (var orderedCommitDefinition in orderedCommitDefinitions)
			{ yield return CreateCommitFromDefinition(orderedCommitDefinition.Item1, orderedCommitDefinition.Item2); }
		}

		/// <summary>
		/// Gets the list of commits from a given blobEntry, starting from a given date
		/// until the end date.
		/// </summary>
		/// <param name="bucketId">The blobEntry id to pull commits from.</param>
		/// <param name="start">The starting date for commits.</param>
		/// <param name="end">The ending date for commits.</param>
		/// <returns>The list of commits from the given blobEntry and greater than or equal to the start date and less than or equal to the end date.</returns>
		public IEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
		{
			var blobs = _blobContainer
				.ListBlobs(GetContainerName() + "/" + bucketId, true,
				BlobListingDetails.Metadata).OfType<CloudPageBlob>();

			// this could be a tremendous amount of data.  Depending on system used
			// this may not be performant enough and may require some sort of index be built.
			var allCommitDefinitions = new List<Tuple<CloudPageBlob, PageBlobCommitDefinition>>();
			foreach (var pageBlob in blobs)
			{
				var header = GetHeader(pageBlob, null);
				foreach (var definition in header.PageBlobCommitDefinitions)
				{
					if (definition.CommitStampUtc >= start && definition.CommitStampUtc <= end)
					{ allCommitDefinitions.Add(new Tuple<CloudPageBlob, PageBlobCommitDefinition>(pageBlob, definition)); }
				}
			}

			// now sort the definitions so we can return out sorted
			var orderedCommitDefinitions = allCommitDefinitions.OrderBy((x) => x.Item2.CommitStampUtc);
			foreach (var orderedCommitDefinition in orderedCommitDefinitions)
			{ yield return CreateCommitFromDefinition(orderedCommitDefinition.Item1, orderedCommitDefinition.Item2); }
		}

		/// <summary>
		/// Gets commits from a given blobEntry and stream id that fall within min and max revisions.
		/// </summary>
		/// <param name="bucketId">The blobEntry id to pull from.</param>
		/// <param name="streamId">The stream id.</param>
		/// <param name="minRevision">The minimum revision.</param>
		/// <param name="maxRevision">The maximum revision.</param>
		/// <returns></returns>
		public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
		{
			int attempts = 0;
			while (true)
			{
				try
				{ return GetFromInternal(bucketId, streamId, minRevision, maxRevision); }
				catch (Microsoft.WindowsAzure.Storage.StorageException ex)
				{
					if (ex.Message.Contains("416"))
					{
						if (attempts++ > 5)
						{
							Logger.Error("Azure is giving us a consistent bounds issue with bucket id: {0}, stream id: {1}.  This may need to be looked into and may indicate corruption with the stream.", bucketId, streamId);
							throw new ConcurrencyException("Issue with azure concurrency.  operation should be tried again", ex);
						}
						else
						{
							Logger.Info("Azure notified us of out of bounds issue.  Occurs occasionally when operating very quickly on single stream.  Storage engine will retry automatically");
							Thread.Sleep(10);
						}
					}
					else
					{ throw; }
				}
			}
		}

		/// <summary>
		/// Internal get from call
		/// </summary>
		/// <param name="bucketId"></param>
		/// <param name="streamId"></param>
		/// <param name="minRevision"></param>
		/// <param name="maxRevision"></param>
		/// <returns></returns>
		private IEnumerable<ICommit> GetFromInternal(string bucketId, string streamId, int minRevision, int maxRevision)
		{
			var commits = new ConcurrentBag<ICommit>();
			var pageBlobReference = _blobContainer
				.ListBlobs(bucketId + "/" + streamId, true,
				BlobListingDetails.Metadata).OfType<CloudPageBlob>().SingleOrDefault();

			if (pageBlobReference == null)
			{ return commits; }

			try
			{
				var header = GetHeader(pageBlobReference, null);

				// find out how many pages we are reading
				int startPage = 0;
				int endPage = startPage;
				int startIndex = 0;
				int numberOfCommits = 0;
				foreach (var commitDefinition in header.PageBlobCommitDefinitions)
				{
					if (minRevision > commitDefinition.Revision)
					{
						++startIndex;
						startPage += commitDefinition.TotalPagesUsed;
					}
					else if (maxRevision < commitDefinition.Revision)
					{ break; }
					else
					{ ++numberOfCommits; }

					endPage += commitDefinition.TotalPagesUsed;
				}

				// download all the data
				var byteContainer = new byte[(endPage - startPage + 1) * 512];
				pageBlobReference.DownloadRangeToByteArray(byteContainer, 0, startPage*512, byteContainer.Length,
					AccessCondition.GenerateIfMatchCondition(pageBlobReference.Properties.ETag));

				// process the downloaded data
				for (int i = startIndex; i != startIndex + numberOfCommits; i++)
				{
					var commitStartIndex = (header.PageBlobCommitDefinitions[(int)i].StartPage - startPage) * _blobPageSize;
					var commitSize = header.PageBlobCommitDefinitions[i].DataSizeBytes;
					using (var ms = new MemoryStream(byteContainer, commitStartIndex, commitSize, false))
					{
						var commit = _serializer.Deserialize<AzureBlobCommit>(ms);
						commits.Add(CreateCommitFromAzureBlobCommit(commit));
					}
				};
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }

			return commits;
		}

		/// <summary>
		/// Gets all undispatched commits across all buckets.
		/// </summary>
		/// <returns>A list of all undispatched commits.</returns>
		public IEnumerable<ICommit> GetUndispatchedCommits()
		{
			// this is most likely extremely inefficient as the size of our store grows to 100's of millions of streams (possibly even just 1000's)
			var containers = _blobClient.ListContainers();
			var allCommitDefinitions = new List<Tuple<CloudPageBlob, PageBlobCommitDefinition>>();

			foreach (var container in containers)
			{
				DateTime sinceDateUtc = DateTime.MinValue;
				try
				{
					// only need to subtract if something different than TimeSpan.MaxValue is specified
					if (!_options.MaxTimeSpanForUndispatched.Equals(TimeSpan.MaxValue))
					{ sinceDateUtc = DateTime.UtcNow.Subtract(_options.MaxTimeSpanForUndispatched); }
				}
				catch (ArgumentOutOfRangeException)
				{ Logger.Info("Date Time was out of range.  falling back to the smallest date/time possible"); }

				// this container is fetched lazily.  so actually filtering down at this level will improve our performance,
				// assuming the options dictate a date range that filters down our set.
				var blobs = container
								.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.Metadata)
								.OfType<CloudPageBlob>().OrderByDescending((x) => x.Properties.LastModified)
								.Where((x) => x.Properties.LastModified > sinceDateUtc);

				// this could be a tremendous amount of data.  Depending on system used
				// this may not be performant enough and may require some sort of index be built.
				foreach (var pageBlob in blobs)
				{
					if (pageBlob.Metadata.ContainsKey(_isEventStreamAggregateKey))
					{
						// we only care about guys who may be dirty
						bool isDirty = true;
						string isDirtyString;
						if (pageBlob.Metadata.TryGetValue(_hasUndispatchedCommitsKey, out isDirtyString))
						{ isDirty = Boolean.Parse(isDirtyString); }

						if (isDirty)
						{
							// Because fetching the header for a specific blob is a two phase operation it may take a couple tries if we are working with the
							// blob.  This is just a quality of life improvement for the user of the store so loading of the store does not hit frequent optimistic
							// concurrency hits that cause the store to have to re-initialize.
							var maxTries = 0;
							while (true)
							{
								try
								{
									var header = GetHeader(pageBlob, null);
									if (header.UndispatchedCommitCount > 0)
									{
										foreach (var definition in header.PageBlobCommitDefinitions)
										{
											if (!definition.IsDispatched)
											{ allCommitDefinitions.Add(new Tuple<CloudPageBlob, PageBlobCommitDefinition>(pageBlob, definition)); }
										}
									}

									break;
								}
								catch (ConcurrencyException)
								{
									if (maxTries++ > 20)
									{
										Logger.Error("Reached max tries for getting undispatched commits and keep receiving concurrency exception.  throwing out.");
										throw;
									}
									else
									{ Logger.Info("Concurrency issue detected while processing undispatched commits.  going to retry to load container"); }
								}
							}
						}
					}
				}
			}

			// now sort the definitions so we can return out sorted
			var orderedCommitDefinitions = allCommitDefinitions.OrderBy((x) => x.Item2.CommitStampUtc);
			foreach (var orderedCommitDefinition in orderedCommitDefinitions)
			{ yield return CreateCommitFromDefinition(orderedCommitDefinition.Item1, orderedCommitDefinition.Item2); }
		}

		/// <summary>
		/// Marks a stream Id's commit as dispatched.
		/// </summary>
		/// <param name="commit">The commit object to mark as dispatched.</param>
		public void MarkCommitAsDispatched(ICommit commit)
		{
			var pageBlobReference = _blobContainer
				.ListBlobs(commit.BucketId + "/" + commit.StreamId, true,
				BlobListingDetails.Metadata).OfType<CloudPageBlob>().SingleOrDefault();

			if (pageBlobReference == null)
			{
				Logger.Warn("tried to mark as dispatched commit that does not exist, commit id: ", commit.CommitId);
				return;
			}

			var headerDefinitionMetadata = GetHeaderDefinitionMetadata(pageBlobReference);
			var header = GetHeader(pageBlobReference, headerDefinitionMetadata);

			// we must commit at a page offset, we will just track how many pages in we must start writing at
			foreach (var commitDefinition in header.PageBlobCommitDefinitions)
			{
				if (commit.CommitId == commitDefinition.CommitId)
				{
					commitDefinition.IsDispatched = true;
					--header.UndispatchedCommitCount;
				}
			}

			CommitNewMessage(pageBlobReference, null, header, headerDefinitionMetadata.HeaderStartLocationOffsetBytes);
		}

		/// <summary>
		/// Purge a container.
		/// </summary>
		public void Purge()
		{ _blobContainer.Delete(); }

		/// <summary>
		/// Purge a series of streams
		/// </summary>
		/// <param name="bucketId"></param>
		public void Purge(string bucketId)
		{
			var blobs = _blobContainer.ListBlobs(GetContainerName() + "/" + bucketId, true, BlobListingDetails.Metadata).OfType<CloudPageBlob>();
			foreach (var blob in blobs)
			{ blob.Delete(); }
		}

		/// <summary>
		/// Drop a container.
		/// </summary>
		public void Drop()
		{ _blobContainer.Delete(); }

		/// <summary>
		/// Deletes a stream by blobEntry and stream id.
		/// </summary>
		/// <param name="bucketId">The blobEntry id.</param>
		/// <param name="streamId">The stream id.</param>
		public void DeleteStream(string bucketId, string streamId)
		{
			var pageBlobReference = _blobContainer.GetPageBlobReference(bucketId + "/" + streamId);
			string leaseId = pageBlobReference.AcquireLease(new TimeSpan(0, 0, 60), null);
			pageBlobReference.Delete(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId));
			pageBlobReference.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId));
		}

		/// <summary>
		/// Disposes this object.
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!disposing || _disposed)
			{
				return;
			}

			Logger.Debug("Disposing...");
			_disposed = true;
		}

		/// <summary>
		/// Adds a commit to a stream.
		/// </summary>
		/// <param name="attempt">The commit attempt to be added.</param>
		/// <returns>An Commit if successful.</returns>
		public ICommit Commit(CommitAttempt attempt)
		{
			var pageBlobReference = _blobContainer
				.ListBlobs(attempt.BucketId + "/" + attempt.StreamId, true,
				BlobListingDetails.Metadata).OfType<CloudPageBlob>().SingleOrDefault();

			if (pageBlobReference == null)
			{ pageBlobReference = CreatePageBlobReference(_blobContainer, attempt.BucketId + "/" + attempt.StreamId); }
			var header = GetHeader(pageBlobReference, null);

			// we must commit at a page offset, we will just track how many pages in we must start writing at
			var startPage = 0;
			foreach (var commit in header.PageBlobCommitDefinitions)
			{
				if (commit.CommitId == attempt.CommitId)
				{ throw new DuplicateCommitException("Duplicate Commit Attempt"); }

				startPage += commit.TotalPagesUsed;
			}

			if (attempt.CommitSequence <= header.LastCommitSequence)
			{ throw new ConcurrencyException("Concurrency exception in Commit"); }

			var blobCommit = new AzureBlobCommit();
			blobCommit.BucketId = attempt.BucketId;
			blobCommit.CommitId = attempt.CommitId;
			blobCommit.CommitSequence = attempt.CommitSequence;
			blobCommit.CommitStampUtc = attempt.CommitStamp;
			blobCommit.Events = attempt.Events.ToList();
			blobCommit.Headers = attempt.Headers;
			blobCommit.StreamId = attempt.StreamId;
			blobCommit.StreamRevision = attempt.StreamRevision;
			var serializedBlobCommit = _serializer.Serialize(blobCommit);

			header.AppendPageBlobCommitDefinition(new PageBlobCommitDefinition(serializedBlobCommit.Length, attempt.CommitId, attempt.StreamRevision,
				attempt.CommitStamp, header.PageBlobCommitDefinitions.Count, startPage, GetNextCheckpoint()));
			++header.UndispatchedCommitCount;
			header.LastCommitSequence = attempt.CommitSequence;

			CommitNewMessage(pageBlobReference, serializedBlobCommit, header, startPage * _blobPageSize);
			return CreateCommitFromAzureBlobCommit(blobCommit);
		}

		/// <summary>
		/// Not yet implemented.  Returns null to ensure functionality.
		/// </summary>
		/// <param name="bucketId"></param>
		/// <param name="streamId"></param>
		/// <param name="maxRevision"></param>
		/// <returns></returns>
		public ISnapshot GetSnapshot(string bucketId, string streamId, int maxRevision)
		{ return null; }

		/// <summary>
		/// Not yet implemented.
		/// </summary>
		/// <param name="snapshot"></param>
		/// <returns></returns>
		public bool AddSnapshot(ISnapshot snapshot)
		{ throw new NotImplementedException(); }

		/// <summary>
		/// Not yet implemented.
		/// </summary>
		/// <param name="bucketId"></param>
		/// <param name="maxThreshold"></param>
		/// <returns></returns>
		public IEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
		{ throw new NotImplementedException(); }

		#region private helpers

		/// <summary>
		/// Creates a commit from the provided definition
		/// </summary>
		/// <param name="streamId"></param>
		/// <param name="commitDefinition"></param>
		/// <returns></returns>
		private ICommit CreateCommitFromDefinition(CloudPageBlob blob, PageBlobCommitDefinition commitDefinition)
		{
			using (var ms = new MemoryStream(commitDefinition.DataSizeBytes))
			{
				var startIndex = commitDefinition.StartPage * 512;
				blob.DownloadRangeToStream(ms, startIndex, commitDefinition.DataSizeBytes, AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag));
				ms.Position = 0;

				AzureBlobCommit azureBlobCommit;
				try
				{ azureBlobCommit = _serializer.Deserialize<AzureBlobCommit>(ms); }
				catch (Exception ex)
				{
					// we hope this does not happen
					var message = string.Format("Blob with uri [{0}] is corrupt.", blob.Uri);
					throw new InvalidDataException(message, ex);
				}

				return CreateCommitFromAzureBlobCommit(azureBlobCommit);
			}
		}

		/// <summary>
		/// Creates a Commit object from an AzureBlobEntry.
		/// </summary>
		/// <param name="blobEntry">The source AzureBlobEntry.</param>
		/// <returns>The populated Commit.</returns>
		private ICommit CreateCommitFromAzureBlobCommit(AzureBlobCommit blobEntry)
		{
			return new Commit(blobEntry.BucketId,
								blobEntry.StreamId,
								blobEntry.StreamRevision,
								blobEntry.CommitId,
								blobEntry.CommitSequence,
								blobEntry.CommitStampUtc,
								blobEntry.Checkpoint.ToString(),
								blobEntry.Headers,
								blobEntry.Events);
		}

		private HeaderDefinitionMetadata GetHeaderDefinitionMetadata(CloudPageBlob blob)
		{
			var headerDefinitionMetaDataOffset = blob.Properties.Length - _headerDefinitionMetadataBytes;
			using (var ms = new MemoryStream(HeaderDefinitionMetadata.RawSize))
			{
				try
				{
					blob.DownloadRangeToStream(ms, headerDefinitionMetaDataOffset, HeaderDefinitionMetadata.RawSize, AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag));
					return HeaderDefinitionMetadata.FromRaw(ms.ToArray());
				}
				catch (Microsoft.WindowsAzure.Storage.StorageException ex)
				{ throw HandleAndRemapCommonExceptions(ex); }
			}
		}

		/// <summary>
		/// Gets the deserialized header from the blob.
		/// </summary>
		/// <param name="blob">The Blob.</param>
		/// <param name="ignoreAccessCondition">true if the access condition check should be ignored... USE LIGHTLY</param>
		/// <returns>A populated StreamBlobHeader.</returns>
		private StreamBlobHeader GetHeader(CloudPageBlob blob, HeaderDefinitionMetadata prefetched)
		{
			try
			{
				var headerDefinitionMetadata = prefetched ?? GetHeaderDefinitionMetadata(blob);
				if (headerDefinitionMetadata.HeaderSizeInBytes != 0)
				{
					using (var ms = new MemoryStream(headerDefinitionMetadata.HeaderSizeInBytes))
					{
						try
						{
							blob.DownloadRangeToStream(ms, headerDefinitionMetadata.HeaderStartLocationOffsetBytes, headerDefinitionMetadata.HeaderSizeInBytes, AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag));
							return _serializer.Deserialize<StreamBlobHeader>(ms.ToArray());
						}
						catch (Microsoft.WindowsAzure.Storage.StorageException ex)
						{ throw HandleAndRemapCommonExceptions(ex); }
					}
				}
				else
				{ return new StreamBlobHeader(); }
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Get page aligned number of bytes from a non page aligned number
		/// </summary>
		/// <param name="nonAligned"></param>
		/// <returns></returns>
		private int GetPageAlignedSize(int nonAligned)
		{
			var remainder = nonAligned % _blobPageSize;
			return (remainder == 0) ? nonAligned : nonAligned + (_blobPageSize - remainder);
		}

		/// <summary>
		/// Resized the blob to a specific size
		/// </summary>
		/// <param name="blob"></param>
		/// <param name="neededSize"></param>
		private void ResizeBlob(CloudPageBlob blob, int neededSize)
		{
			try
			{
				var currentSize = blob.Properties.Length;
				var newSize = GetPageAlignedSize(neededSize);
				blob.Resize(newSize, AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag));
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Commits the header information which essentially commits any transactions that occured
		/// related to that header.
		/// </summary>
		/// <param name="newCommit">the new commit to write</param>
		/// <param name="headerStartOffsetBytes">where in the blob the new commit will be written</param>
		/// <param name="blob">blob header applies to</param>
		/// <param name="header">the new header data</param>
		/// <returns></returns>
		private void CommitNewMessage(CloudPageBlob blob, byte[] newCommit, StreamBlobHeader header, int offsetBytes)
		{
			try
			{
				newCommit = newCommit ?? new byte[0];

				blob.Metadata[_isEventStreamAggregateKey] = "yes";
				blob.Metadata[_hasUndispatchedCommitsKey] = header.PageBlobCommitDefinitions.Any((x) => !x.IsDispatched).ToString();
				blob.SetMetadata(AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag));

				var serialized = _serializer.Serialize(header);
				var writeStartLocationAligned = GetPageAlignedSize(offsetBytes);
				var totalSpaceNeededAligned = GetPageAlignedSize(writeStartLocationAligned + newCommit.Length + serialized.Length + _headerDefinitionMetadataBytes);

				var totalBlobLength = blob.Properties.Length;
				if (totalBlobLength < totalSpaceNeededAligned)
				{
					ResizeBlob(blob, totalSpaceNeededAligned);
					totalBlobLength = blob.Properties.Length;
				}

				var writeSize = totalBlobLength - writeStartLocationAligned;
				using (var ms = new MemoryStream((int)writeSize))
				{
					ms.Write(newCommit, 0, newCommit.Length);
					ms.Write(serialized, 0, serialized.Length);

					var remainder = (ms.Position % _blobPageSize);
					var fillSpace = writeSize - newCommit.Length - serialized.Length - _headerDefinitionMetadataBytes;
					ms.Write(_maxFillSpace, 0, (int)fillSpace);

					var headerDefinitionMetadata = new HeaderDefinitionMetadata();
					headerDefinitionMetadata.HasUndispatchedCommits = header.PageBlobCommitDefinitions.Any((x) => !x.IsDispatched);
					headerDefinitionMetadata.HeaderSizeInBytes = serialized.Length;
					headerDefinitionMetadata.HeaderStartLocationOffsetBytes = writeStartLocationAligned + newCommit.Length;
					var rawHeaderDefinitionMetadata = headerDefinitionMetadata.GetRaw();
					ms.Write(rawHeaderDefinitionMetadata, 0, rawHeaderDefinitionMetadata.Length);
					fillSpace = _headerDefinitionMetadataBytes - rawHeaderDefinitionMetadata.Length;
					ms.Write(_maxFillSpace, 0, (int)fillSpace);

					ms.Position = 0;
					blob.WritePages(ms, writeStartLocationAligned, accessCondition: AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag));
				}
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{ throw HandleAndRemapCommonExceptions(ex); }
		}

		/// <summary>
		/// Build the container name.
		/// </summary>
		/// <returns>The container name.</returns>
		private string GetContainerName()
		{ return _eventSourcePrefix + _options.ContainerName.ToLower(); }

		/// <summary>
		/// Tries to fetch a blob's attributes.  Creates the blob if it does not exist.
		/// </summary>
		/// <param name="blob">The blob.</param>
		private CloudPageBlob CreatePageBlobReference(CloudBlobContainer cloudBlobContainer, string blobName)
		{
			var pageBlobReference = cloudBlobContainer.GetPageBlobReference(blobName);
			pageBlobReference.Create((long)512);
			pageBlobReference.FetchAttributes();
			return pageBlobReference;
		}

		/// <summary>
		/// Gets the next checkpoint id
		/// </summary>
		/// <returns></returns>
		private uint GetNextCheckpoint()
		{
			var blobContainer2 = _blobClient.GetContainerReference(_rootContainerName);

			var pageBlobReference = _blobContainer
				.ListBlobs(_checkpointBlobName, true,
				BlobListingDetails.Metadata).OfType<CloudPageBlob>().SingleOrDefault();

			if (pageBlobReference == null)
			{ pageBlobReference = CreatePageBlobReference(blobContainer2, _checkpointBlobName); }

			string serializedCheckpointNumber;
			uint nextCheckpoint = 1;
			if (pageBlobReference.Metadata.TryGetValue(_checkpointNumberKey, out serializedCheckpointNumber))
			{ nextCheckpoint = Convert.ToUInt32(serializedCheckpointNumber) + 1; }

			while (true)
			{
				try
				{
					pageBlobReference.Metadata[_checkpointNumberKey] = (nextCheckpoint).ToString();
					pageBlobReference.SetMetadata(AccessCondition.GenerateIfMatchCondition(pageBlobReference.Properties.ETag));
					break;
				}
				catch (Microsoft.WindowsAzure.Storage.StorageException ex)
				{
					if (ex.Message.Contains("412"))
					{
						if (_options.ForceUniqueCheckpoint)
						{
							pageBlobReference.FetchAttributes();

							if (pageBlobReference.Metadata.TryGetValue(_checkpointNumberKey, out serializedCheckpointNumber))
							{ nextCheckpoint = Convert.ToUInt32(serializedCheckpointNumber) + 1; }
						}
						else
						{ break; }
					}
					else
					{ throw; }
				}
			}

			return nextCheckpoint;
		}

		private Exception HandleAndRemapCommonExceptions(Microsoft.WindowsAzure.Storage.StorageException ex)
		{
			if (ex.Message.Contains("412"))
			{ return new ConcurrencyException("concurrency exception in markcommitasdispachted", ex); }
			else
			{ return ex; }
		}

		#endregion
	}
}