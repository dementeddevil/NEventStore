using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// Type of container to create/use.
	/// </summary>
	public enum eAzureBlobContainerTypes
	{
		/// <summary>
		/// Unknown container type.
		/// </summary>
		UnknownType = 0,

		/// <summary>
		/// Container for Aggregates.
		/// </summary>
		AggregateStream = 1,

		/// <summary>
		/// Container for sagas.
		/// </summary>
		SagaStream = 2
	}
}
