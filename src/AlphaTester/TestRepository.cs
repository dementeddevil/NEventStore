using System;
using System.Collections.Generic;
using System.Linq;
using NEventStore.CommonDomain.Core;
using NEventStore.CommonDomain.Persistence.EventStore;
using NEventStore;

namespace AlphaTester
{
    using System.Threading;

    public class TestRepository : NEventStoreRepositoryBase
	{
		private static readonly object _lockObject = new object();
		private static IStoreEvents _storeEvents = null;

		private EventStoreRepository _repository;

		public TestRepository(eRepositoryType repositoryType)
			:base(repositoryType)
		{ }

		/// <summary>
		/// Get the specified aggregate by id
		/// </summary>
		/// <typeparam name="TAggregate">The type of aggregate to get</typeparam>
		/// <param name="id">the id of the aggregate to get</param>
		/// <param name="version">version to get... use 0 to get the latest version</param>
		/// <returns></returns>
		public SimpleAggregate GetSimpleAggregateById( Guid id, int version )
		{
			if ( id == Guid.Empty )
			{
				throw new InvalidOperationException( "Guid.Empty is not a valid Guid" );
			}

			LazyInit( ref _storeEvents, _lockObject );
			return _repository.GetByIdAsync<SimpleAggregate>( id, version, CancellationToken.None ).GetAwaiter().GetResult();
		}

		public List<ICommit> GetSimpleAggregateFromTo( DateTime start, DateTime end )
		{

			LazyInit( ref _storeEvents, _lockObject );
			return _storeEvents.Advanced.GetFromToAsync( Bucket.Default, start, end, CancellationToken.None ).GetAwaiter().GetResult().ToList();
		}

		/// <summary>
		/// Save the specified aggregate
		/// </summary>
		/// <param name="aggregate">aggregate to save</param>
		/// <param name="commitId">the commit id</param>
		/// <param name="updateHeaders">update headers</param>
		public void Save( SimpleAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders )
		{
			if ( aggregate == null )
			{ throw new ArgumentNullException( "aggregate" ); }

			if ( aggregate.Id == Guid.Empty )
			{
				throw new InvalidOperationException( "Guid.Empty is not a valid id for an aggregate" );
			}

			LazyInit( ref _storeEvents, _lockObject );
			_repository.SaveAsync( aggregate, commitId, updateHeaders, CancellationToken.None ).GetAwaiter().GetResult();
		}


        public void Snapshot(SimpleAggregate aggregate)
        {
            var memento = aggregate.prepareMemento();
            _storeEvents.Advanced.AddSnapshotAsync(new Snapshot(aggregate.Id.ToString(), memento.Version, memento), CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void LazyInit( ref IStoreEvents storeEventsInstance, object lockObject )
		{
			base.LazyInit( ref storeEventsInstance, lockObject );

			if ( _repository == null )
			{ _repository = new EventStoreRepository( storeEventsInstance, new AggregateFactory(), new ConflictDetector() ); }
		}

		/// <summary>
		/// 
		/// </summary>
		public void Dispose()
		{
			if ( _repository != null )
			{
				_repository.Dispose();
			}
		}
	}
}
