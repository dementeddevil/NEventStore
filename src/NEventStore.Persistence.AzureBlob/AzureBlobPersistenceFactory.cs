using NEventStore.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
    class AzureBlobPersistenceFactory : IPersistenceFactory
    {
        private readonly string _connectionString;
        private readonly IDocumentSerializer _serializer;
		private readonly AzureBlobPersistenceOptions _options;

        public AzureBlobPersistenceFactory(string connectionString, IDocumentSerializer serializer, AzureBlobPersistenceOptions options = null)
        {
            _connectionString = connectionString;
            _serializer = serializer;
	        _options = options ?? new AzureBlobPersistenceOptions();
        }

        public IPersistStreams Build()
        { return new AzureBlobPersistenceEngine(); }
    }
}
