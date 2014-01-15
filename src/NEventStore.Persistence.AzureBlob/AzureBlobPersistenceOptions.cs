using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
		/// Create a new AzureBlobPersistenceOptions
		/// </summary>
		/// <param name="containerName">name of the container within the azure storage account</param>
		/// <param name="containerType">typeof container</param>
		/// <param name="defaultStartingBlobSizeKb">the starting size of a new blob that is created.  this is useful if you know the general size of your blobs.</param>
		/// <param name="blobGrowthRate">the growth rate of the blob when it needs more space.  A value of 1 will cause the blob to grow by exactly what it requires</param>
		public AzureBlobPersistenceOptions(
			string containerName = "default",
			eAzureBlobContainerTypes containerType = eAzureBlobContainerTypes.UnknownType,
			int defaultStartingBlobSizeKb = 1, double blobGrowthRate = 1)
		{
			if (blobGrowthRate < 1)
			{ throw new ArgumentOutOfRangeException("blobGrowthRate", "blob growth rate must be greater than or equal to 1"); }

			ContainerName = containerName;
			ContainerType = containerType != eAzureBlobContainerTypes.UnknownType ? containerType : eAzureBlobContainerTypes.AggregateStream;
			DefaultStartingBlobSizeKb = defaultStartingBlobSizeKb;
		}
	}
}
