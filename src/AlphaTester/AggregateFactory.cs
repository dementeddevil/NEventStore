using CommonDomain;
using CommonDomain.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlphaTester
{
	/// <summary>
	/// Factory for aggregates
	/// </summary>
	public class AggregateFactory : IConstructAggregates
	{
		/// <summary>
		/// Build the aggregate instance given the provided snapshot
		/// </summary>
		/// <param name="type">type of aggregate</param>
		/// <param name="id">id of the aggregate</param>
		/// <param name="snapshot">snapshot to load from</param>
		/// <returns></returns>
		public IAggregate Build( Type type, Guid id, IMemento snapshot )
		{
            var instance = Activator.CreateInstance( type ) as IAggregate;

            var aggregate = instance as SimpleAggregate;
            aggregate.ApplySnapshot(snapshot);
            return aggregate;
        }
	}
}
