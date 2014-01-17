using System;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Holds options to be used initializing the engine.
    /// </summary>
    public class AzureBlobPersistenceOptions
    {
        /// <summary>
        /// Type of the container to created/used.
        /// </summary>
        public eAzureBlobContainerTypes ContainerType
        { get; private set; }

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
        public AzureBlobPersistenceOptions(
            string containerName = "default",
            eAzureBlobContainerTypes containerType = eAzureBlobContainerTypes.UnknownType,
            int parallelConnectionLimit = 10,
            int defaultStartingBlobSizeKb = 1, double blobGrowthRate = 1)
        {
            if (blobGrowthRate < 1)
            { throw new ArgumentOutOfRangeException("blobGrowthRate", "blob growth rate must be greater than or equal to 1"); }

            ContainerName = containerName;
            ContainerType = containerType != eAzureBlobContainerTypes.UnknownType ? containerType : eAzureBlobContainerTypes.AggregateStream;
            DefaultStartingBlobSizeKb = defaultStartingBlobSizeKb;
            ParallelConnectionLimit = parallelConnectionLimit;
        }
    }
}
