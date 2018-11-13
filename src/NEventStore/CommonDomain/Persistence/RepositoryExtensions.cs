using System;
using System.Threading;

namespace NEventStore.CommonDomain.Persistence
{
    public static class RepositoryExtensions
	{
		public static void SaveAsync(this IRepository repository, IAggregate aggregate, Guid commitId, CancellationToken cancellationToken)
		{
			repository.SaveAsync(aggregate, commitId, a => { }, cancellationToken);
		}

		public static void SaveAsync(this IRepository repository, string bucketId, IAggregate aggregate, Guid commitId, CancellationToken cancellationToken)
		{
			repository.SaveAsync(bucketId, aggregate, commitId, a => { }, cancellationToken);
		}
	}
}