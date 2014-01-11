using NEventStore.Logging;
using NEventStore.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Threading;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace NEventStore.Persistence.AzureBlob
{
	public class AzureBlobPersistenceEngine : IPersistStreams
	{
		private static readonly ILog Logger = LogFactory.BuildLogger( typeof( AzureBlobPersistenceEngine ) );

        public const string _headerMetadataKey = "header";

		private string _connectionString;
		private ISerialize _serializer;
		private AzureBlobPersistenceOptions _options;
		private CloudStorageAccount _storageAccount;
		private CloudBlobClient _blobClient;
		private CloudBlobContainer _blobContainer;
		private int _initialized;
		private bool _disposed;

		public AzureBlobPersistenceEngine( string connectionString, ISerialize serializer, AzureBlobPersistenceOptions options = null )
		{
			if ( String.IsNullOrEmpty( connectionString ) )
			{ throw new ArgumentException( "connectionString cannot be null or empty" ); }

			if ( serializer == null )
			{ throw new ArgumentNullException( "serializer" ); }

			if ( options == null )
			{ throw new ArgumentNullException( "options" ); }

			_connectionString = connectionString;
			_serializer = serializer;
			_options = options;
		}

		public bool IsDisposed
		{ get { return _disposed; } }

		public void Initialize()
		{
			if ( Interlocked.Increment( ref _initialized ) > 1 )
			{ return; }

			_storageAccount = CloudStorageAccount.Parse( _connectionString );
			_blobClient = _storageAccount.CreateCloudBlobClient();
			_blobContainer = _blobClient.GetContainerReference( GetContainerName() );
			_blobContainer.CreateIfNotExists();
		}

		private string GetContainerName()
		{
			string containerSuffix = _options.ContainerType.ToString().ToLower();
			return _options.ContainerName.ToLower() + containerSuffix;
		}

        public ICheckpoint GetCheckpoint(string checkpointToken = null)
        { throw new NotImplementedException(); }

		public IEnumerable<ICommit> GetFrom( string bucketId, DateTime start )
		{ return GetFromTo(bucketId, start, DateTime.MaxValue); }

		public IEnumerable<ICommit> GetFrom( string checkpointToken = null )
		{ throw new NotImplementedException("support for get from leveraging checkpoints is not yet implemented"); }

		public IEnumerable<ICommit> GetFromTo( string bucketId, DateTime start, DateTime end )
        { throw new NotImplementedException("support for get from leveraging dates is not yet implemented"); }

        public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            // once again ignoring lease.
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + bucketId + "/" + streamId);
            CreateIfNotExistsAndFetchAttributes(pageBlobReference);
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
                    continue;
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
            var commits = new List<ICommit>();
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
                        var bucket = _serializer.Deserialize<BlobBucket>(ms2);
                        commits.Add(CreateCommitFromBlobBucket(bucket));

                    }

                    var remainder = header.PageBlobCommitDefinitions[i].DataSizeBytes % 512;
                    ms.Read(byteContainer, 0, 512 - remainder);
                }
            }

            return commits;
        }

		public IEnumerable<ICommit> GetUndispatchedCommits()
		{
            // this is most likely extremely ineficcient as the size of our store grows to 100's of millions of aggregates (possibly even just 1000's)
            var sw = new Stopwatch();
            sw.Start();
            var blobs = _blobContainer
                            .ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.Metadata)
                            .OfType<CloudPageBlob>();

            List<ICommit> commits = new List<ICommit>();
            long containerSizeBytes = 0;
            foreach (var blob in blobs)
            {
                var header = GetHeader(blob);
                var pageIndex = 0;
                containerSizeBytes += blob.Properties.Length;
                foreach (var blobDefinition in header.PageBlobCommitDefinitions)
                {
                    if (!blobDefinition.IsDispatched)
                    {
                        var startIndexBytes = pageIndex * 512;
                        var commitBytes = new byte[blobDefinition.DataSizeBytes];

                        using (var ms = new MemoryStream(blobDefinition.DataSizeBytes))
                        {
                            blob.DownloadRangeToStream(ms, startIndexBytes, blobDefinition.DataSizeBytes);
                            ms.Position = 0;

                            BlobBucket bucket;
                            try
                            { bucket = _serializer.Deserialize<BlobBucket>(ms); }
                            catch (Exception ex)
                            {
                                // we hope this does not happen
                                var message = string.Format("Blob with uri [{0}] is corrupt.", blob.Uri);
                                throw new InvalidDataException(message, ex);
                            }

                            commits.Add(CreateCommitFromBlobBucket(bucket));
                        }
                    }
                    pageIndex += blobDefinition.TotalPagesUsed;
                }
            }

            Console.WriteLine("Found [{0}] undispatched commits in [{1}] ms.  container size is [{2}] bytes", commits.Count(), sw.ElapsedMilliseconds, containerSizeBytes);
			return commits;
		}

		public void MarkCommitAsDispatched( ICommit commit )
		{
            // get the blob... not worrying about leasing yet, but i should :)
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + commit.BucketId + "/" + commit.StreamId);
            CreateIfNotExistsAndFetchAttributes(pageBlobReference);
            var header = GetHeader(pageBlobReference);

            // we must commit at a page offset, we will just track how many pages in we must start writing at
            foreach (var commitDefinition in header.PageBlobCommitDefinitions)
            {
                if (commit.CommitId == commit.CommitId)
                { commitDefinition.IsDispatched = true; }
            }

            pageBlobReference.Metadata[_headerMetadataKey] = Convert.ToBase64String(_serializer.Serialize(header));
            pageBlobReference.SetMetadata();
		}

		public void Purge()
		{ _blobContainer.Delete(); }

		public void Purge( string bucketId )
		{
			throw new NotImplementedException();
		}

		public void Drop()
        { _blobContainer.Delete(); }

		public void DeleteStream( string bucketId, string streamId )
		{
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + bucketId + "/" + streamId);
            pageBlobReference.Delete();
		}

		public void Dispose()
		{
			Dispose( true );
			GC.SuppressFinalize( this );
		}

		protected virtual void Dispose( bool disposing )
		{
			if ( !disposing || _disposed )
			{
				return;
			}

			Logger.Debug( "Disposing..." );
			_disposed = true;
		}

		public ICommit Commit( CommitAttempt attempt )
		{
            // get the blob... not worrying about leasing yet
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + attempt.BucketId + "/" + attempt.StreamId);
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

            var bucket = new BlobBucket();
            bucket.BucketId = attempt.BucketId;
            bucket.CommitId = attempt.CommitId;
            bucket.CommitSequence = attempt.CommitSequence;
            bucket.CommitStamp = attempt.CommitStamp;
            bucket.Events = attempt.Events.ToList();
            bucket.Headers = attempt.Headers;
            bucket.StreamId = attempt.StreamId;
            bucket.StreamRevision = attempt.StreamRevision;
            bucket.Items = attempt.Events.Count;
            var serializedBucket = _serializer.Serialize(bucket);

            var remainder = serializedBucket.Length % 512;
            var newBucket = new byte[serializedBucket.Length + (512-remainder)];
            Array.Copy(serializedBucket, newBucket, serializedBucket.Length);

            header.AppendPageBlobCommitDefinition(new PageBlobCommitDefinition(serializedBucket.Length, attempt.CommitId, attempt.StreamRevision, attempt.CommitStamp));
            using (var ms = new MemoryStream(newBucket, false))
            {
                // if the header write fails, we will throw out.  the application will need to try again.  it will be as if
                // this commit never succeeded.  we need to also autogrow the page blob if we are going to exceed its max.
                var bytesRequired = startPage * 512 + ms.Length;
                if (pageBlobReference.Properties.Length < bytesRequired)
                {
                    var currentSize = pageBlobReference.Properties.Length;
                    var newSize = Math.Max((long)(currentSize * _options.BlobGrowthRatePercent), bytesRequired);
                    var remainder2 = newSize % 512;
                    newSize = newSize + 512 - remainder2;
                    pageBlobReference.Resize(newSize);
                }

                pageBlobReference.WritePages(ms, startPage * 512);
                pageBlobReference.Metadata[_headerMetadataKey] = Convert.ToBase64String(_serializer.Serialize(header));
                pageBlobReference.SetMetadata();
            }

            return new Commit(
                    attempt.BucketId,
                    attempt.StreamId,
                    attempt.StreamRevision,
                    attempt.CommitId,
                    attempt.CommitSequence,
                    attempt.CommitStamp,
                    // Hardcode checkpoint for now
                    "1",
                    attempt.Headers,
                    attempt.Events );
        }

		public ISnapshot GetSnapshot( string bucketId, string streamId, int maxRevision )
		{
			return null;
		}

		public bool AddSnapshot( ISnapshot snapshot )
		{
			throw new NotImplementedException();
		}

		public IEnumerable<IStreamHead> GetStreamsToSnapshot( string bucketId, int maxThreshold )
		{
			throw new NotImplementedException();
		}

        #region private helpers

        private ICommit CreateCommitFromBlobBucket(BlobBucket bucket)
        {
            var commit = new Commit(bucket.BucketId,
                                       bucket.StreamId,
                                       bucket.StreamRevision,
                                       bucket.CommitId,
                                       bucket.CommitSequence,
                                       bucket.CommitStamp,
                                       "1",
                                       bucket.Headers,
                                       bucket.Events);
            return commit;
        }

        private PageBlobHeader GetHeader(CloudPageBlob blob)
        {
            string serializedHeader;
            blob.Metadata.TryGetValue(_headerMetadataKey, out serializedHeader);

            var header = new PageBlobHeader();
            if (serializedHeader != null)
            { header = _serializer.Deserialize<PageBlobHeader>(Convert.FromBase64String(serializedHeader)); }
            return header;
        }

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
                    // this should be defined by an option
                    blob.Create(1024 * _options.DefaultStartingBlobSizeKb);
                    blob.FetchAttributes();
                }
                else
                { throw; }
            }
        }

        #endregion
    }
}
