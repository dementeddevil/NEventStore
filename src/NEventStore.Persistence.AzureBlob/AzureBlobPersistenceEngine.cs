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
			return _options.ContainerName.ToLower() +containerSuffix;
		}

		public IEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ICommit> GetFrom(string checkpointToken = null)
		{
			throw new NotImplementedException();
		}

		public ICheckpoint GetCheckpoint(string checkpointToken = null)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ICommit> GetUndispatchedCommits()
		{
			var blobs = _blobContainer
			.ListBlobs( useFlatBlobListing: true, blobListingDetails: BlobListingDetails.Metadata )
			.OfType<CloudBlockBlob>()
			.Where( b => b.Metadata[dispatchedMetadataKey] == "false" );

			List<ICommit> commits = new List<ICommit>();
			foreach ( CloudBlockBlob blob in blobs )
			{
				if ( blob.Metadata[dispatchedMetadataKey].Equals( "false", StringComparison.InvariantCultureIgnoreCase ) )
				{
					BlobBucket bucket;
					using ( var stream = new MemoryStream() )
					{
						blob.DownloadToStream( stream );
						var serializedBucket = stream.ToArray();
						bucket = _serializer.Deserialize<BlobBucket>( serializedBucket );
					}
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
			} 
			return commits;
		}

		public void MarkCommitAsDispatched(ICommit commit)
		{
			BlobBucket bucket;
			Logger.Debug( "Marking commit id {0} for stream id {1} as dispatched", commit.CommitId, commit.StreamId );

			var directory = _blobContainer.GetDirectoryReference( GetContainerName() + "/" + commit.BucketId + "/" + commit.StreamId );

			var blob = directory.ListBlobs( blobListingDetails: BlobListingDetails.Metadata ).OfType<CloudBlockBlob>()
							.SingleOrDefault( b => b.Metadata["CommitSequence"] == commit.CommitSequence.ToString() ); 

			if ( blob != null )
			{
				using ( var stream = new MemoryStream() )
				{
					blob.DownloadToStream( stream );
					var serializedBucket = stream.ToArray();
					bucket = _serializer.Deserialize<BlobBucket>( serializedBucket );
				}
				bucket.Dispatched = true;

				var blockBlobReference = _blobContainer.GetBlockBlobReference( GetContainerName() + "/"
											+ blob.Metadata[bucketIdMetadataKey] + "/"
											+ blob.Metadata[streamIdMetadataKey] + "/" 
											+ blob.Metadata[commitSequenceMetadataKey] );

				blockBlobReference.Metadata[bucketIdMetadataKey] = blob.Metadata[bucketIdMetadataKey];
				blockBlobReference.Metadata[streamIdMetadataKey] = blob.Metadata[streamIdMetadataKey];
				blockBlobReference.Metadata[commitSequenceMetadataKey] = blob.Metadata[commitSequenceMetadataKey];
				blockBlobReference.Metadata[streamRevisionMetadataKey] = blob.Metadata[streamRevisionMetadataKey];
				blockBlobReference.Metadata[commitStampMetadataKey] = blob.Metadata[commitStampMetadataKey];
				blockBlobReference.Metadata[dispatchedMetadataKey] = "true";

				var newSerializedBucker = _serializer.Serialize( bucket );
				using ( var stream = new MemoryStream( newSerializedBucker ) )
				{
					try
					{ blockBlobReference.UploadFromStream( stream ); }
					catch ( Exception ex )
					{ Logger.Warn( "failed to mark as dispatched. exception was {0}", ex ); }
				}
			}
		}

		public void Purge()
		{
			throw new NotImplementedException();
		}

		public void Purge(string bucketId)
		{
			throw new NotImplementedException();
		}

		public void Drop()
		{
			throw new NotImplementedException();
		}

		public void DeleteStream(string bucketId, string streamId)
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

		public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
		{
			Logger.Debug( "getting from minrevision {0} to maxrevision {1}", minRevision, maxRevision );

			var directory = _blobContainer.GetDirectoryReference( GetContainerName() + "/" + bucketId + "/" + streamId );

			var blobs = directory.ListBlobs( blobListingDetails: BlobListingDetails.Metadata ).OfType<CloudBlockBlob>();

			//var blobs = blobContainer.ListBlobs( blobListingDetails: BlobListingDetails.Metadata );
			//Console.WriteLine( string.Format( "Call to GetFrom ListBlobs [{0}] ms", sw.ElapsedMilliseconds ) );

			var po = new ParallelOptions();
			po.MaxDegreeOfParallelism = Math.Min(8, Math.Max(1, blobs.Count()));

			ConcurrentBag<ICommit> commits = new ConcurrentBag<ICommit>();
			Parallel.ForEach( blobs, po,
				blob =>
				{
					var revision = Int32.Parse( blob.Metadata[streamRevisionMetadataKey] );
					if ( revision >= minRevision )
					{
						BlobBucket bucket;
						using ( var stream = new MemoryStream() )
						{
							blob.DownloadToStream( stream );
							var serializedBucket = stream.ToArray();
							bucket = _serializer.Deserialize<BlobBucket>( serializedBucket );
						}
						if ( bucket.StreamRevision - bucket.Items < maxRevision )
						{
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
					}
			});
			IOrderedEnumerable<ICommit> sorted = commits.ToList().OrderBy( c => c.StreamRevision );
			return sorted;
		}

		public ICommit Commit(CommitAttempt attempt)
		{
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

			var serializedBucket = _serializer.Serialize( bucket );

			var blockBlobReference = _blobContainer.GetBlockBlobReference( GetContainerName() + "/" + attempt.BucketId + "/" + attempt.StreamId + "/" + attempt.CommitSequence );

			blockBlobReference.Metadata[bucketIdMetadataKey] = attempt.BucketId;
			blockBlobReference.Metadata[streamIdMetadataKey] = attempt.StreamId;
			blockBlobReference.Metadata[commitSequenceMetadataKey] = attempt.CommitSequence.ToString();
			blockBlobReference.Metadata[streamRevisionMetadataKey] = attempt.StreamRevision.ToString();
			blockBlobReference.Metadata[commitStampMetadataKey] = attempt.CommitStamp.ToString( "u" );
			blockBlobReference.Metadata[dispatchedMetadataKey] = "false";

			using ( var stream = new MemoryStream( serializedBucket ) )
			{
				try
				{
					blockBlobReference.UploadFromStream( stream, AccessCondition.GenerateIfNoneMatchCondition( "*" ) );
				}
				catch ( StorageException ex )
				{
					if ( ex.Message.Contains( "(409) Conflict" ) )
					{ throw new DuplicateCommitException( "Duplicate Commit Attempt", ex ); }
					else
					{ throw; }
				}
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

		public ISnapshot GetSnapshot(string bucketId, string streamId, int maxRevision)
		{
			return null;
		}

		public bool AddSnapshot(ISnapshot snapshot)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
		{
			throw new NotImplementedException();
		}
	}
}
