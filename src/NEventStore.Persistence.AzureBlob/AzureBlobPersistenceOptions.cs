using System;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// Holds options to be used initializing the engine.
	/// </summary>
	public class AzureBlobPersistenceOptions
	{
		/// <summary>
		/// Name of the container to be created/used.
		/// </summary>
		public string ContainerName
		{ get; private set; }

		/// <summary>
		/// Get the default starting blob size for a new blob
		/// </summary>
		public int DefaultStartingBlobSizeKb
		{ get; private set; }

		/// <summary>
		/// Get the rate teh blob should grow at when it needs more space
		/// </summary>
		public double BlobGrowthRatePercent
		{ get; private set; }

		/// <summary>
		/// Get if unique checkpoints should be enforced.
		/// </summary>
		/// <remarks>
		/// unique checkpoints mean no commit will ever have the same checkpoint.  in order to accomplish this
		/// a performance hit will be taken on high throughput systems given they must optimistically generate a checkpoint.
		/// if your system is ok with some level of duplicate, yet incrementing checkpoints, this value should be set to false
		/// to improve system performance.
		/// </remarks>
		public bool ForceUniqueCheckpoint
		{ get; private set; }

		/// <summary>
		/// Get the maximum number of parallel rest connections that can be made to the blob storage at once.
		/// </summary>
		/// <remarks>
		/// this value is actually a .NET API limitor derived from the ServicePointManager class.  This value will
		/// update the ServicePointManager connection limit just for the blob storage URI requests.  No other system
		/// requests will be effected
		/// </remarks>
		public int ParallelConnectionLimit
		{ get; set; }

		/// <summary>
		/// Create a new AzureBlobPersistenceOptions
		/// </summary>
		/// <param name="containerName">name of the container within the azure storage account</param>
		/// <param name="containerType">typeof container</param>
		/// <param name="parallelConnectionLimit">maximum parallel connection that can be made to the storage account at once</param>
		/// <param name="defaultStartingBlobSizeKb">the starting size of a new blob that is created.  this is useful if you know the general size of your blobs.</param>
		/// <param name="blobGrowthRate">the growth rate of the blob when it needs more space.  A value of 1 will cause the blob to grow by exactly what it requires</param>
		/// <param name="forceUniqueCheckpoint">force unique checkpoint</param>
		public AzureBlobPersistenceOptions(
			string containerName = "default",
			int parallelConnectionLimit = 10,
			int defaultStartingBlobSizeKb = 1, double blobGrowthRate = 1,
			bool forceUniqueCheckpoint = true)
		{
			if (blobGrowthRate < 1)
			{ throw new ArgumentOutOfRangeException("blobGrowthRate", "blob growth rate must be greater than or equal to 1"); }

			ContainerName = containerName;
			DefaultStartingBlobSizeKb = defaultStartingBlobSizeKb;
			ParallelConnectionLimit = parallelConnectionLimit;
			ForceUniqueCheckpoint = forceUniqueCheckpoint;
		}
	}
}
