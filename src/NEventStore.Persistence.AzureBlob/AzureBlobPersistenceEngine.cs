using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NEventStore.Logging;
using NEventStore.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

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
		private const string _headerMetadataKey = "header";

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
					var header = GetHeader(pageBlob);
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
				var header = GetHeader(pageBlob);
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
						if (attempts++ > 100)
						{
							Logger.Fatal("Azure is giving us a consistent bounds issue with bucket id: {0}, stream id: {1}.  This may need to be looked into and may indicate corruption with the stream.", bucketId, streamId);
							throw;
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

		private IEnumerable<ICommit> GetFromInternal(string bucketId, string streamId, int minRevision, int maxRevision)
		{
			var commits = new List<ICommit>();
			var pageBlobReference = _blobContainer.GetPageBlobReference(bucketId + "/" + streamId);
			try
			{
				pageBlobReference.FetchAttributes();
				var header = GetHeader(pageBlobReference);

				// find out how many pages we are reading
				int startPage = 0;
				int endPage = 0;
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
				var totalBytes = (endPage - startPage + 1) * 512;
				var byteContainer = new byte[totalBytes];
				using (var ms = new MemoryStream(totalBytes))
				{
					var offset = startPage * 512;
					pageBlobReference.DownloadRangeToStream(ms, offset, totalBytes);
					ms.Position = 0;

					// now walk it and make it so
					for (int i = startIndex; i != startIndex + numberOfCommits; ++i)
					{
						ms.Read(byteContainer, 0, header.PageBlobCommitDefinitions[i].DataSizeBytes);
						using (var ms2 = new MemoryStream(byteContainer, 0, header.PageBlobCommitDefinitions[i].DataSizeBytes, false))
						{
							var blobCommit = _serializer.Deserialize<AzureBlobCommit>(ms2);
							commits.Add(CreateCommitFromAzureBlobCommit(blobCommit));
						}

						var remainder = header.PageBlobCommitDefinitions[i].DataSizeBytes % 512;
						ms.Read(byteContainer, 0, 512 - remainder);
					}
				}
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{
				if (ex.Message.Contains("404"))
				{ Logger.Warn("tried to get from stream that does not exist, stream id:  ", streamId); }
				else
				{ throw; }
			}

			return commits.OrderBy(c => c.StreamRevision);
		}

		/// <summary>
		/// Gets all undispatched commits across all buckets.
		/// </summary>
		/// <returns>A list of all undispatched commits.</returns>
		public IEnumerable<ICommit> GetUndispatchedCommits()
		{
			// this is most likely extremely ineficcient as the size of our store grows to 100's of millions of streams (possibly even just 1000's)
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
					var header = GetHeader(pageBlob);
					if (header.UndispatchedCommitCount > 0)
					{
						foreach (var definition in header.PageBlobCommitDefinitions)
						{
							if (!definition.IsDispatched)
							{ allCommitDefinitions.Add(new Tuple<CloudPageBlob, PageBlobCommitDefinition>(pageBlob, definition)); }
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
			var pageBlobReference = _blobContainer.GetPageBlobReference(commit.BucketId + "/" + commit.StreamId);

			try
			{
				pageBlobReference.FetchAttributes();
				string eTag = pageBlobReference.Properties.ETag;
				var header = GetHeader(pageBlobReference);

				// we must commit at a page offset, we will just track how many pages in we must start writing at
				foreach (var commitDefinition in header.PageBlobCommitDefinitions)
				{
					if (commit.CommitId == commitDefinition.CommitId)
					{
						commitDefinition.IsDispatched = true;
						--header.UndispatchedCommitCount;
					}
				}
				pageBlobReference.Metadata[_headerMetadataKey] = Convert.ToBase64String(_serializer.Serialize(header));
				
				try
				{ pageBlobReference.SetMetadata(AccessCondition.GenerateIfMatchCondition(eTag)); }
				catch (Microsoft.WindowsAzure.Storage.StorageException ex)
				{
					if (ex.Message.Contains("412"))
					{ throw new ConcurrencyException("concurrency exception in markcommitasdispachted", ex); }
					else
					{ throw; }
				}
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{
				if (ex.Message.Contains("404"))
				{ Logger.Warn("tried to mark as dispatched commit that does not exist, commit id: ", commit.CommitId); }
				else
				{ throw; }
			}
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
			var pageBlobReference = _blobContainer.GetPageBlobReference(attempt.BucketId.ToLower() + "/" + attempt.StreamId);
			CreateIfNotExistsAndFetchAttributes(pageBlobReference);
			var header = GetHeader(pageBlobReference);

			// we must commit at a page offset, we will just track how many pages in we must start writing at
			var startPage = 0;
			foreach (var commit in header.PageBlobCommitDefinitions)
			{
				if (commit.CommitId == attempt.CommitId)
				{ throw new DuplicateCommitException("Duplicate Commit Attempt"); }

				startPage += commit.TotalPagesUsed;
			}

			if (attempt.CommitSequence <= header.LastCommitSequence)
			{ throw new ConcurrencyException(); }

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

			var remainder = serializedBlobCommit.Length % 512;
			var pageAlignedBlobCommit = new byte[serializedBlobCommit.Length + (512 - remainder)];
			Array.Copy(serializedBlobCommit, pageAlignedBlobCommit, serializedBlobCommit.Length);

			header.AppendPageBlobCommitDefinition(new PageBlobCommitDefinition(serializedBlobCommit.Length, attempt.CommitId, attempt.StreamRevision,
				attempt.CommitStamp, header.PageBlobCommitDefinitions.Count, startPage, GetNextCheckpoint()));
			++header.UndispatchedCommitCount;
			header.LastCommitSequence = attempt.CommitSequence;

			try
			{
				using (var ms = new MemoryStream(pageAlignedBlobCommit))
				{
					// if the header write fails, we will throw out.  the application will need to try again.  it will be as if
					// this commit never succeeded.  we need to also autogrow the page blob if we are going to exceed its max.
					var bytesRequired = startPage * 512 + ms.Length;
					if (pageBlobReference.Properties.Length < bytesRequired)
					{
						var currentSize = pageBlobReference.Properties.Length;
						var newSize = Math.Max((long)(currentSize * _options.BlobGrowthRatePercent), bytesRequired);
						var remainder2 = newSize % 512;
						if (remainder2 != 0)
						{ newSize = newSize + 512 - remainder2; }

						pageBlobReference.Resize(newSize, AccessCondition.GenerateIfMatchCondition(pageBlobReference.Properties.ETag));
					}

					pageBlobReference.WritePages(ms, startPage * 512, accessCondition: AccessCondition.GenerateIfMatchCondition(pageBlobReference.Properties.ETag));
					pageBlobReference.Metadata[_headerMetadataKey] = Convert.ToBase64String(_serializer.Serialize(header));
					pageBlobReference.SetMetadata(AccessCondition.GenerateIfMatchCondition(pageBlobReference.Properties.ETag));
				}
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{
				if (ex.Message.Contains("412"))
				{ throw new ConcurrencyException(); }
				throw;
			}

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
			var header = GetHeader(blob);
						
			using (var ms = new MemoryStream(commitDefinition.DataSizeBytes))
			{
				var startIndex = commitDefinition.StartPage * 512;
				blob.DownloadRangeToStream(ms, startIndex, commitDefinition.DataSizeBytes);
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
			return new Commit(  blobEntry.BucketId,
								blobEntry.StreamId,
								blobEntry.StreamRevision,
								blobEntry.CommitId,
								blobEntry.CommitSequence,
								blobEntry.CommitStampUtc,
								blobEntry.Checkpoint.ToString(),
								blobEntry.Headers,
								blobEntry.Events);
		}

		/// <summary>
		/// Gets the deserialized header from the blob.
		/// </summary>
		/// <param name="blob">The Blob.</param>
		/// <returns>A populated StreamBlobHeader.</returns>
		private StreamBlobHeader GetHeader(CloudPageBlob blob)
		{
			string serializedHeader;
			blob.Metadata.TryGetValue(_headerMetadataKey, out serializedHeader);

			var header = new StreamBlobHeader();
			if (serializedHeader != null)
			{ header = _serializer.Deserialize<StreamBlobHeader>(Convert.FromBase64String(serializedHeader)); }
			return header;
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
		private void CreateIfNotExistsAndFetchAttributes(CloudPageBlob blob)
		{
			try
			{
				blob.FetchAttributes();
			}
			catch (Microsoft.WindowsAzure.Storage.StorageException ex)
			{
				if (ex.Message.Contains("404"))
				{
					blob.Create(1024 * _options.DefaultStartingBlobSizeKb);
					blob.FetchAttributes();
				}
				else
				{ throw; }
			}
		}

		/// <summary>
		/// Gets the next checkpoint id
		/// </summary>
		/// <returns></returns>
		private uint GetNextCheckpoint()
		{
			var blobContainer2 = _blobClient.GetContainerReference(_rootContainerName);
			var pageBlobReference = blobContainer2.GetPageBlobReference(_checkpointBlobName);
			pageBlobReference.FetchAttributes();

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

			//Console.WriteLine("using checkpoint number [{0}]", nextCheckpoint);
			return nextCheckpoint;
		}

		#endregion
	}
}