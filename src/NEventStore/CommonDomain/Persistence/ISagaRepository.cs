namespace CommonDomain.Persistence
{
	using System;
	using System.Collections.Generic;
	using System.Threading;
	using System.Threading.Tasks;

    public interface ISagaRepository
	{
		Task<TSaga> GetByIdAsync<TSaga>(Guid sagaId, CancellationToken cancellationToken) where TSaga : class, ISaga, new();

	    Task<TSaga> GetByIdAsync<TSaga>(string bucketId, Guid sagaId, CancellationToken cancellationToken) where TSaga : class, ISaga, new();

		Task SaveAsync(ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken);

	    Task SaveAsync(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken);
	}
}