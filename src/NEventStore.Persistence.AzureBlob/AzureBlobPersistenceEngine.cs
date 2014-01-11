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

		public const string bucketIdMetadataKey = "BucketId";
		public const string streamIdMetadataKey = "StreamId";
		public const string commitSequenceMetadataKey = "CommitSequence";
		public const string streamRevisionMetadataKey = "StreamRevision";
		public const string commitStampMetadataKey = "CommitStamp";
		public const string dispatchedMetadataKey = "Dispatched";

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

		public IEnumerable<ICommit> GetFrom( string bucketId, DateTime start )
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ICommit> GetFrom( string checkpointToken = null )
		{
			throw new NotImplementedException();
		}

		public ICheckpoint GetCheckpoint( string checkpointToken = null )
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ICommit> GetFromTo( string bucketId, DateTime start, DateTime end )
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ICommit> GetUndispatchedCommits()
		{
            //var blobs = _blobContainer
            //                .ListBlobs( useFlatBlobListing: true, blobListingDetails: BlobListingDetails.Metadata )
            //                .OfType<CloudBlockBlob>()
            //                .Where( b => b.Metadata[dispatchedMetadataKey] == "false" );

			List<ICommit> commits = new List<ICommit>();
			//foreach ( CloudBlockBlob blob in blobs )
			//{
			//	if ( blob.Metadata[dispatchedMetadataKey].Equals( "false", StringComparison.InvariantCultureIgnoreCase ) )
			//	{
			//		BlobBucket bucket;
			//		using ( var stream = new MemoryStream() )
			//		{
			//			blob.DownloadToStream( stream );
			//			var serializedBucket = stream.ToArray();
			//			bucket = _serializer.Deserialize<BlobBucket>( serializedBucket );
			//		}
			//		var commit = new Commit( bucket.BucketId,
			//								bucket.StreamId,
			//								bucket.StreamRevision,
			//								bucket.CommitId,
			//								bucket.CommitSequence,
			//								bucket.CommitStamp,
			//								"1",
			//								bucket.Headers,
			//								bucket.Events );
			//		commits.Add( commit );
			//	}
			//} 
			return commits;
		}

		public void MarkCommitAsDispatched( ICommit commit )
		{
            // get the blob... not worrying about leasing yet
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + commit.BucketId + "/" + commit.StreamId);
            CreateIfNotExistsAndFetchAttributes(pageBlobReference);

            string serializedHeader;
            pageBlobReference.Metadata.TryGetValue("header", out serializedHeader);

            var header = new PageBlobHeader();
            if (serializedHeader != null)
            { header = _serializer.Deserialize<PageBlobHeader>(Convert.FromBase64String(serializedHeader)); }

            // we must commit at a page offset, we will just track how many pages in we must start writing at
            foreach (var commitDefinition in header.PageBlobCommitDefinitions)
            {
                if (commit.CommitId == commit.CommitId)
                { commitDefinition.IsDispatched = true; }
            }

            pageBlobReference.Metadata["header"] = Convert.ToBase64String(_serializer.Serialize(header));
            pageBlobReference.SetMetadata();
		}

		public void Purge()
		{
			throw new NotImplementedException();
		}

		public void Purge( string bucketId )
		{
			throw new NotImplementedException();
		}

		public void Drop()
		{
			throw new NotImplementedException();
		}

		public void DeleteStream( string bucketId, string streamId )
		{
			throw new NotImplementedException();
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

		public IEnumerable<ICommit> GetFrom( string bucketId, string streamId, int minRevision, int maxRevision )
		{
            // once again ignoring lease.
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + bucketId + "/" + streamId);
            CreateIfNotExistsAndFetchAttributes(pageBlobReference);

            string serializedHeader;
            pageBlobReference.Metadata.TryGetValue("header", out serializedHeader);

            var header = new PageBlobHeader();
            if (serializedHeader != null)
            { header = _serializer.Deserialize<PageBlobHeader>(Convert.FromBase64String(serializedHeader)); }

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
                for (int i = startIndex; i != startIndex+numberOfCommits; ++i)
                {
                    ms.Read(byteContainer, 0, header.PageBlobCommitDefinitions[i].DataSizeBytes);
                    using (var ms2 = new MemoryStream(byteContainer, 0, header.PageBlobCommitDefinitions[i].DataSizeBytes, false))
                    {
                        var bucket = _serializer.Deserialize<BlobBucket>(ms2);

                        var commit = new Commit( bucket.BucketId,
									bucket.StreamId,
									bucket.StreamRevision,
									bucket.CommitId,
									bucket.CommitSequence,
									bucket.CommitStamp,
									"1",
									bucket.Headers,
									bucket.Events );
					    commits.Add( commit );

                    }

                    var remainder = header.PageBlobCommitDefinitions[i].DataSizeBytes % 512;
                    ms.Read(byteContainer, 0, 512-remainder);
                }
            }

            return commits;
		}

		public ICommit Commit( CommitAttempt attempt )
		{
            // get the blob... not worrying about leasing yet
            var pageBlobReference = _blobContainer.GetPageBlobReference(GetContainerName() + "/" + attempt.BucketId + "/" + attempt.StreamId);
            CreateIfNotExistsAndFetchAttributes(pageBlobReference);

            string serializedHeader;
            pageBlobReference.Metadata.TryGetValue("header", out serializedHeader);

            var header = new PageBlobHeader();
            if (serializedHeader != null)
            { header = _serializer.Deserialize<PageBlobHeader>(Convert.FromBase64String(serializedHeader)); }

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

            header.AppendPageBlobCommitDefinition(new PageBlobCommitDefinition(serializedBucket.Length, attempt.CommitId, attempt.StreamRevision));
            using (var ms = new MemoryStream(newBucket, false))
            {
                // if the header write fails, we will throw out.  the application will need to try again.  it will be as if
                // this commit never succeeded.  we need to also autogrow the page blob if we are going to exceed its max.  we grow by
                // 50% or the minimum new size required, whichever is greater
                var bytesRequired = startPage * 512 + ms.Length;
                if (pageBlobReference.Properties.Length < bytesRequired)
                {
                    var currentSize = pageBlobReference.Properties.Length;
                    var newSize = Math.Max((long)(currentSize * 1.5), bytesRequired);
                    var remainder2 = newSize % 512;
                    newSize = newSize + 512 - remainder2;
                    pageBlobReference.Resize(newSize);
                }

                pageBlobReference.WritePages(ms, startPage * 512);
                pageBlobReference.Metadata["header"] = Convert.ToBase64String(_serializer.Serialize(header));
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
                    blob.Create(1024 * 1);
                    blob.Metadata["header"] = Convert.ToBase64String(_serializer.Serialize(new PageBlobHeader()));
                    blob.FetchAttributes();
                }
                else
                { throw; }
            }
        }
	}
}
