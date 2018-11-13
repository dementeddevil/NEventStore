using System;

namespace NEventStore.CommonDomain
{
    public interface IMemento
	{
		Guid Id { get; set; }

		int Version { get; set; }
	}
}