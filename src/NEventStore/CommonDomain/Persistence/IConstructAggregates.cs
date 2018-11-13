using System;

namespace NEventStore.CommonDomain.Persistence
{
    public interface IConstructAggregates
	{
		IAggregate Build(Type type, Guid id, IMemento snapshot);
	}
}