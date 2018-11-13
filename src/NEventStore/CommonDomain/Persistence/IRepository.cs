using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NEventStore.CommonDomain.Persistence
{
    public interface IRepository : IDisposable
	{
	    Task<TAggregate> GetByIdAsync<TAggregate>(Guid id, CancellationToken cancellationToken) where TAggregate : class, IAggregate;

	    Task<TAggregate> GetByIdAsync<TAggregate>(Guid id, int version, CancellationToken cancellationToken) where TAggregate : class, IAggregate;

	    Task<TAggregate> GetByIdAsync<TAggregate>(string bucketId, Guid id, CancellationToken cancellationToken) where TAggregate : class, IAggregate;

	    Task<TAggregate> GetByIdAsync<TAggregate>(string bucketId, Guid id, int version, CancellationToken cancellationToken) where TAggregate : class, IAggregate;

		Task SaveAsync(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken);

		Task SaveAsync(string bucketId, IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken);
	}
}