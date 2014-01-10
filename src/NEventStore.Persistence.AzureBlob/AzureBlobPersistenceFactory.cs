using NEventStore.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
	public class AzureBlobPersistenceFactory : IPersistenceFactory
	{
		private readonly string _connectionString;
		private readonly ISerialize _serializer;
		private readonly AzureBlobPersistenceOptions _options;

		public AzureBlobPersistenceFactory( string connectionString, ISerialize serializer, AzureBlobPersistenceOptions options = null )
		{
			_connectionString = connectionString;
			_serializer = serializer;
			_options = options ?? new AzureBlobPersistenceOptions();
		}

		public IPersistStreams Build()
		{ return new AzureBlobPersistenceEngine(_connectionString, _serializer, _options); }
	}
}
