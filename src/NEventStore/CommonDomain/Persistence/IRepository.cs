namespace CommonDomain.Persistence
{
	using System;
	using System.Collections.Generic;
	using System.Threading.Tasks;

	public interface IRepository : IDisposable
	{
		Task<TAggregate> GetById<TAggregate>(Guid id) where TAggregate : class, IAggregate;

		Task<TAggregate> GetById<TAggregate>(Guid id, int version) where TAggregate : class, IAggregate;

		Task<TAggregate> GetById<TAggregate>(string bucketId, Guid id) where TAggregate : class, IAggregate;

		Task<TAggregate> GetById<TAggregate>(string bucketId, Guid id, int version) where TAggregate : class, IAggregate;

		Task Save(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders);

		Task Save(string bucketId, IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders);
	}
}