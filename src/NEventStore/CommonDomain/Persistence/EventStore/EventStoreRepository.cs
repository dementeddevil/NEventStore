namespace CommonDomain.Persistence.EventStore
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading;
	using System.Threading.Tasks;
	using NEventStore;
	using NEventStore.Persistence;

	public class EventStoreRepository : IRepository, IDisposable
	{
		private const string AggregateTypeHeader = "AggregateType";

		private readonly IDetectConflicts _conflictDetector;

		private readonly IStoreEvents _eventStore;

		private readonly IConstructAggregates _factory;

		private readonly IDictionary<string, ISnapshot> _snapshots = new Dictionary<string, ISnapshot>();

		private readonly IDictionary<string, IEventStream> _streams = new Dictionary<string, IEventStream>();

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

		public virtual Task<TAggregate> GetByIdAsync<TAggregate>(Guid id, CancellationToken cancellationToken) where TAggregate : class, IAggregate
		{
			return GetByIdAsync<TAggregate>(Bucket.Default, id, cancellationToken);
		}

		public virtual Task<TAggregate> GetByIdAsync<TAggregate>(Guid id, int versionToLoad, CancellationToken cancellationToken) where TAggregate : class, IAggregate
		{
			return GetByIdAsync<TAggregate>(Bucket.Default, id, versionToLoad, cancellationToken);
		}

		public Task<TAggregate> GetByIdAsync<TAggregate>(string bucketId, Guid id, CancellationToken cancellationToken) where TAggregate : class, IAggregate
		{
			return GetByIdAsync<TAggregate>(bucketId, id, int.MaxValue, cancellationToken);
		}

		public async Task<TAggregate> GetByIdAsync<TAggregate>(string bucketId, Guid id, int versionToLoad, CancellationToken cancellationToken) where TAggregate : class, IAggregate
		{
			var snapshot = await GetSnapshotAsync(bucketId, id, versionToLoad, cancellationToken).ConfigureAwait(false);
			var stream = await OpenStreamAsync(bucketId, id, versionToLoad, snapshot, cancellationToken).ConfigureAwait(false);
			var aggregate = GetAggregate<TAggregate>(snapshot, stream);

			ApplyEventsToAggregate(versionToLoad, stream, aggregate);

			return aggregate as TAggregate;
		}

		public virtual Task SaveAsync(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken)
		{
			return SaveAsync(Bucket.Default, aggregate, commitId, updateHeaders, cancellationToken);
		}

		public async Task SaveAsync(string bucketId, IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken)
		{
			var headers = PrepareHeaders(aggregate, updateHeaders);
			while (true)
			{
				var stream = await PrepareStreamAsync(bucketId, aggregate, headers, cancellationToken).ConfigureAwait(false);
				var commitEventCount = stream.CommittedEvents.Count;

				try
				{
					await stream.CommitChangesAsync(commitId, cancellationToken).ConfigureAwait(false);
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
                    stream.ClearChanges();
                    
                    if (ThrowOnConflict(stream, commitEventCount))
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
			var memento = snapshot?.Payload as IMemento;
			return _factory.Build(typeof(TAggregate), stream.StreamId.ToGuid(), memento);
		}

		private async Task<ISnapshot> GetSnapshotAsync(string bucketId, Guid id, int version, CancellationToken cancellationToken)
		{
		    var snapshotId = bucketId + id;
			if (!_snapshots.TryGetValue(snapshotId, out var snapshot))
			{
				_snapshots[snapshotId] = snapshot = (await _eventStore
				    .Advanced
				    .GetSnapshotAsync(bucketId, id, version, cancellationToken)
				    .ConfigureAwait(false));
			}

			return snapshot;
		}

		private async Task<IEventStream> OpenStreamAsync(string bucketId, Guid id, int version, ISnapshot snapshot, CancellationToken cancellationToken)
		{
		    var streamsId = bucketId + "+" + id;
			if (_streams.TryGetValue(streamsId, out var stream))
			{
				return stream;
			}

			stream = snapshot == null
                ? await _eventStore.OpenStreamAsync(bucketId, id, cancellationToken, 0, version).ConfigureAwait(false)
                : await _eventStore.OpenStreamAsync(snapshot, version, cancellationToken).ConfigureAwait(false);

			return _streams[streamsId] = stream;
		}

		private async Task<IEventStream> PrepareStreamAsync(string bucketId, IAggregate aggregate, Dictionary<string, object> headers, CancellationToken cancellationToken)
		{
		    var streamsId = bucketId + "+" + aggregate.Id;
			if (!_streams.TryGetValue(streamsId, out var stream))
			{
				_streams[streamsId] = stream = (await _eventStore
				    .CreateStreamAsync(bucketId, aggregate.Id, cancellationToken)
				    .ConfigureAwait(false));
			}

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
		    updateHeaders?.Invoke(headers);

		    return headers;
		}

		private bool ThrowOnConflict(IEventStream stream, int skip)
		{
			var committed = stream.CommittedEvents.Skip(skip).Select(x => x.Body);
			var uncommitted = stream.UncommittedEvents.Select(x => x.Body);
			return _conflictDetector.ConflictsWith(uncommitted, committed);
		}
	}
}