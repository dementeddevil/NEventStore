namespace CommonDomain.Persistence.EventStore
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using NEventStore;
	using NEventStore.Persistence;

	public class SagaEventStoreRepository : ISagaRepository, IDisposable
	{
		private const string SagaTypeHeader = "SagaType";

		private const string UndispatchedMessageHeader = "UndispatchedMessage.";

		private readonly IStoreEvents _eventStore;

		private readonly IConstructSagas _factory;
		private readonly ConcurrentDictionary<string, Task<IEventStream>> _streams = new ConcurrentDictionary<string, Task<IEventStream>>();

		public SagaEventStoreRepository(IStoreEvents eventStore, IConstructSagas factory)
		{
			_eventStore = eventStore;
			_factory = factory;
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public async Task<TSaga> GetById<TSaga>(string bucketId, string sagaId) where TSaga : class, ISaga
		{
            return BuildSaga<TSaga>(sagaId, await OpenStream(bucketId, sagaId));
		}

        public async Task Save(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
		{
			if (saga == null)
			{
				throw new ArgumentNullException("saga", ExceptionMessages.NullArgument);
			}

			Dictionary<string, object> headers = PrepareHeaders(saga, updateHeaders);
            IEventStream stream = await PrepareStream(bucketId, saga, headers);

            await Persist(stream, commitId);

			saga.ClearUncommittedEvents();
			saga.ClearUndispatchedMessages();
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

				_streams.Clear();
			}
		}

		private Task<IEventStream> OpenStream(string bucketId, string sagaId)
		{
            var sagaKey = bucketId + "+" + sagaId;
			return _streams.GetOrAdd(
				sagaKey,
				(sk) =>
				{
					try
					{
						return _eventStore.OpenStream(bucketId, sagaId, 0, int.MaxValue);
					}
					catch (StreamNotFoundException)
					{
						return _eventStore.CreateStream(bucketId, sagaId);
					}
				});
		}

		private TSaga BuildSaga<TSaga>(string sagaId, IEventStream stream) where TSaga : class, ISaga
		{
			var saga = (TSaga)_factory.Build(typeof(TSaga), sagaId);
			foreach (var @event in stream.CommittedEvents.Select(x => x.Body))
			{
				saga.Transition(@event);
			}

			saga.ClearUncommittedEvents();
			saga.ClearUndispatchedMessages();

			return saga;
		}

		private static Dictionary<string, object> PrepareHeaders(
			ISaga saga, Action<IDictionary<string, object>> updateHeaders)
		{
			var headers = new Dictionary<string, object>();

			headers[SagaTypeHeader] = saga.GetType().FullName;
			if (updateHeaders != null)
			{
				updateHeaders(headers);
			}

			int i = 0;
			foreach (var command in saga.GetUndispatchedMessages())
			{
				headers[UndispatchedMessageHeader + i++] = command;
			}

			return headers;
		}

		private async Task<IEventStream> PrepareStream(string bucketId, ISaga saga, Dictionary<string, object> headers)
		{
            var sagaKey = bucketId + "+" + saga.Id;
			IEventStream stream = await _streams.GetOrAdd(
				sagaKey,
				(sid)=> _eventStore.CreateStream(bucketId, saga.Id));

			foreach (var item in headers)
			{
				stream.UncommittedHeaders[item.Key] = item.Value;
			}

			saga.GetUncommittedEvents().Cast<object>().Select(x => new EventMessage { Body = x }).ToList().ForEach(stream.Add);

			return stream;
		}

		private static async Task Persist(IEventStream stream, Guid commitId)
		{
			try
			{
				await stream.CommitChanges(commitId);
			}
			catch (DuplicateCommitException)
			{
				stream.ClearChanges();
			}
			catch (StorageException e)
			{
				throw new PersistenceException(e.Message, e);
			}
		}
	}
}