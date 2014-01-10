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

		public AzureBlobPersistenceOptions(string containerName = null, eAzureBlobContainerTypes containerType = eAzureBlobContainerTypes.UnknownType)
		{
			ContainerName = !String.IsNullOrEmpty(containerName) ? containerName : "enrollment";
			ContainerType = containerType != eAzureBlobContainerTypes.UnknownType ? containerType : eAzureBlobContainerTypes.AggregateStream;
		}
	}
}
