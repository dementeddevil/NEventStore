using NEventStore.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
	public static class AzureBlobPersistenceWireupExtensions
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="wireup">wireup being extended</param>
		/// <param name="azureConnectionString">the connection string for the azure storage</param>
		/// <param name="serializer">type of serializer to use</param>
		/// <param name="options">options for the azure persistence engine</param>
		/// <returns>An AzureBlobPersistenceWireup</returns>
		public static AzureBlobPersistenceWireup UsingAzureBlobPersistence(this Wireup wireup, string azureConnectionString, ISerialize serializer, AzureBlobPersistenceOptions options = null)
		{ return new AzureBlobPersistenceWireup(wireup, azureConnectionString, serializer, options); }
	}
}
