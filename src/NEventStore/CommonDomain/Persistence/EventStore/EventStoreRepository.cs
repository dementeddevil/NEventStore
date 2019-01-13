namespace CommonDomain.Persistence.EventStore
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using NEventStore;
	using NEventStore.Persistence;

    public class EventStoreRepository : IRepository
	{
		private const string AggregateTypeHeader = "AggregateType";

		private readonly IDetectConflicts _conflictDetector;

		private readonly IStoreEvents _eventStore;

		private readonly IConstructAggregates _factory;

		private readonly ConcurrentDictionary<string, Task<ISnapshot>> _snapshots = new ConcurrentDictionary<string, Task<ISnapshot>>();

		private readonly ConcurrentDictionary<string, Task<IEventStream>> _streams = new ConcurrentDictionary<string, Task<IEventStream>>();

		public EventStoreRepository(IStoreEvents eventStore, IConstructAggregates factory, IDetectConflicts conflictDetector)
		{
			_eventStore = eventStore;
			_factory = factory;
			_conflictDetector = conflictDetector;
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public virtual Task<TAggregate> GetById<TAggregate>(Guid id) where TAggregate : class, IAggregate
		{
			return GetById<TAggregate>(Bucket.Default, id);
		}

		public virtual Task<TAggregate> GetById<TAggregate>(Guid id, int versionToLoad) where TAggregate : class, IAggregate
		{
			return GetById<TAggregate>(Bucket.Default, id, versionToLoad);
		}

		public Task<TAggregate> GetById<TAggregate>(string bucketId, Guid id) where TAggregate : class, IAggregate
		{
			return GetById<TAggregate>(bucketId, id, int.MaxValue);
		}

		public async Task<TAggregate> GetById<TAggregate>(string bucketId, Guid id, int versionToLoad) where TAggregate : class, IAggregate
		{
			ISnapshot snapshot = await GetSnapshot(bucketId, id, versionToLoad);
			IEventStream stream = await OpenStream(bucketId, id, versionToLoad, snapshot);
			IAggregate aggregate = GetAggregate<TAggregate>(snapshot, stream);

			ApplyEventsToAggregate(versionToLoad, stream, aggregate);

			return aggregate as TAggregate;
		}

		public virtual Task Save(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
		{
			return Save(Bucket.Default, aggregate, commitId, updateHeaders);
		}

		public async Task Save(string bucketId, IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
		{
			Dictionary<string, object> headers = PrepareHeaders(aggregate, updateHeaders);
			while (true)
			{
				IEventStream stream = await PrepareStream(bucketId, aggregate, headers);
				int commitEventCount = stream.CommittedEvents.Count;

				try
				{
					await stream.CommitChanges(commitId);
					aggregate.ClearUncommittedEvents();
					return;
				}
				catch (DuplicateCommitException)
				{
					stream.ClearChanges();
					return;
				}
				catch (ConcurrencyException e)
				{
                    var conflict = ThrowOnConflict(stream, commitEventCount);
                    stream.ClearChanges();

                    if (conflict)
					{
						throw new ConflictingCommandException(e.Message, e);
					}
				}
				catch (StorageException e)
				{
					throw new PersistenceException(e.Message, e);
				}
			}
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!disposing)
			{
				return;
			}

			lock (_streams)
			{
				foreach (var stream in _streams)
				{
					stream.Value.Dispose();
				}

				_snapshots.Clear();
				_streams.Clear();
			}
		}

		private static void ApplyEventsToAggregate(int versionToLoad, IEventStream stream, IAggregate aggregate)
		{
			if (versionToLoad == 0 || aggregate.Version < versionToLoad)
			{
				foreach (var @event in stream.CommittedEvents.Select(x => x.Body))
				{
					aggregate.ApplyEvent(@event);
				}
			}
		}

		private IAggregate GetAggregate<TAggregate>(ISnapshot snapshot, IEventStream stream)
		{
			IMemento memento = snapshot == null ? null : snapshot.Payload as IMemento;
			return _factory.Build(typeof(TAggregate), stream.StreamId.ToGuid(), memento);
		}

		private async Task<ISnapshot> GetSnapshot(string bucketId, Guid id, int version)
		{
			var snapshotId = bucketId + id;

			return await _snapshots.GetOrAdd(
				snapshotId,
				(sid) => _eventStore.Advanced.GetSnapshot(bucketId, id, version));
		}

		private Task<IEventStream> OpenStream(string bucketId, Guid id, int version, ISnapshot snapshot)
		{
			var streamId = bucketId + "+" + id;
			return _streams.GetOrAdd(
				streamId,
				(sid) => snapshot == null
					? _eventStore.OpenStream(bucketId, id, 0, version)
					: _eventStore.OpenStream(snapshot, version));
		}

		private async Task<IEventStream> PrepareStream(string bucketId, IAggregate aggregate, Dictionary<string, object> headers)
		{
			var streamId = bucketId + "+" + aggregate.Id;
			var stream = await _streams.GetOrAdd(
				streamId,
				(sid) => _eventStore.CreateStream(bucketId, aggregate.Id));

			foreach (var item in headers)
			{
				stream.UncommittedHeaders[item.Key] = item.Value;
			}

			aggregate.GetUncommittedEvents()
			         .Cast<object>()
			         .Select(x => new EventMessage { Body = x })
			         .ToList()
			         .ForEach(stream.Add);

			return stream;
		}

		private static Dictionary<string, object> PrepareHeaders(
			IAggregate aggregate, Action<IDictionary<string, object>> updateHeaders)
		{
			var headers = new Dictionary<string, object>();

			headers[AggregateTypeHeader] = aggregate.GetType().FullName;
			if (updateHeaders != null)
			{
				updateHeaders(headers);
			}

			return headers;
		}

		private bool ThrowOnConflict(IEventStream stream, int skip)
		{
			IEnumerable<object> committed = stream.CommittedEvents.Skip(skip).Select(x => x.Body);
			IEnumerable<object> uncommitted = stream.UncommittedEvents.Select(x => x.Body);
			return _conflictDetector.ConflictsWith(uncommitted, committed);
		}
	}
}