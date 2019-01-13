namespace CommonDomain.Persistence
{
	using System;
	using System.Collections.Generic;
	using System.Threading.Tasks;

    public interface ISagaRepository
	{
		Task<TSaga> GetById<TSaga>(string bucketId, string sagaId) where TSaga : class, ISaga;

		Task Save(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders);
	}
}