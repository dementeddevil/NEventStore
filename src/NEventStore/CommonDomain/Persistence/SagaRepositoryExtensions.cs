namespace CommonDomain.Persistence
{
    using System;
    using System.Collections.Generic;
	using System.Threading.Tasks;
    using NEventStore;

    public static class SagaRepositoryExtensions
    {
        public static Task<TSaga> GetById<TSaga>(this ISagaRepository sagaRepository, Guid sagaId)
            where TSaga : class, ISaga, new()
        {
            return sagaRepository.GetById<TSaga>(Bucket.Default, sagaId.ToString());
        }

        public static Task Save(
            this ISagaRepository sagaRepository,
            ISaga saga,
            Guid commitId,
            Action<IDictionary<string, object>> updateHeaders)
        {
            return sagaRepository.Save(Bucket.Default, saga, commitId, updateHeaders);
        }

        public static Task<TSaga> GetById<TSaga>(this ISagaRepository sagaRepository, string sagaId)
            where TSaga : class, ISaga
        {
            return sagaRepository.GetById<TSaga>(Bucket.Default, sagaId);
        }
    }
}