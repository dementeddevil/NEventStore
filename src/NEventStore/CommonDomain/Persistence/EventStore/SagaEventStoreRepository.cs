using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NEventStore.Persistence;

namespace NEventStore.CommonDomain.Persistence.EventStore
{
    public class SagaEventStoreRepository : ISagaRepository, IDisposable
    {
        private const string SagaTypeHeader = "SagaType";
        private const string UndispatchedMessageHeader = "UndispatchedMessage.";
        private readonly IStoreEvents _eventStore;
        private readonly IConstructSagas _factory;
        private readonly IDictionary<string, IEventStream> _streams = new Dictionary<string, IEventStream>();

        public SagaEventStoreRepository(IStoreEvents eventStore, IConstructSagas factory)
        {
            Guard.NotNull(() => eventStore, eventStore);
            Guard.NotNull(() => factory, factory);
            _eventStore = eventStore;
            _factory = factory;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public Task<TSaga> GetByIdAsync<TSaga>(Guid sagaId, CancellationToken cancellationToken) where TSaga : class, ISaga, new()
        {
            return GetByIdAsync<TSaga>(Bucket.Default, sagaId, cancellationToken);
        }

        public async Task<TSaga> GetByIdAsync<TSaga>(string bucketId, Guid sagaId, CancellationToken cancellationToken) where TSaga : class, ISaga, new()
        {
            var saga = GetSaga<TSaga>();
            ApplyEventsToSaga(await OpenStreamAsync(bucketId, sagaId, cancellationToken).ConfigureAwait(false), saga);
            return saga as TSaga;
        }

        public Task SaveAsync(ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken)
        {
            return SaveAsync(Bucket.Default, saga, commitId, updateHeaders, cancellationToken);
        }

        public async Task SaveAsync(string bucketId, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders, CancellationToken cancellationToken)
        {
            if (saga == null)
            {
                throw new ArgumentNullException(nameof(saga), ExceptionMessages.NullArgument);
            }

            var headers = PrepareHeaders(saga, updateHeaders);
            var stream = await PrepareStream(bucketId, saga, headers, cancellationToken).ConfigureAwait(false);

            await PersistAsync(stream, commitId, cancellationToken).ConfigureAwait(false);

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

        private async Task<IEventStream> OpenStreamAsync(string bucketId, Guid sagaId, CancellationToken cancellationToken)
        {
            var streamsId = bucketId + sagaId;

            if (_streams.TryGetValue(streamsId, out var stream))
            {
                return stream;
            }

            try
            {
                stream = await _eventStore.OpenStreamAsync(bucketId, sagaId, cancellationToken, 0/*, int.MaxValue*/).ConfigureAwait(false);
            }
            catch (StreamNotFoundException)
            {
                stream = await _eventStore.CreateStreamAsync(sagaId, cancellationToken).ConfigureAwait(false);
            }

            return _streams[streamsId] = stream;
        }

        private ISaga GetSaga<TSaga>() where TSaga : class, ISaga, new()
        {
            return _factory.Build(typeof(TSaga));
        }

        private static void ApplyEventsToSaga(IEventStream stream, ISaga saga)
        {
            foreach (var @event in stream.CommittedEvents.Select(x => x.Body))
            {
                saga.Transition(@event);
            }

            saga.ClearUncommittedEvents();
            saga.ClearUndispatchedMessages();
        }

        private static Dictionary<string, object> PrepareHeaders(
            ISaga saga, Action<IDictionary<string, object>> updateHeaders)
        {
            var headers = new Dictionary<string, object>();

            headers[SagaTypeHeader] = saga.GetType().FullName;
            updateHeaders?.Invoke(headers);

            var i = 0;
            foreach (var command in saga.GetUndispatchedMessages())
            {
                headers[UndispatchedMessageHeader + i++] = command;
            }

            return headers;
        }

        private async Task<IEventStream> PrepareStream(string bucketId, ISaga saga, Dictionary<string, object> headers, CancellationToken cancellationToken)
        {
            var streamsId = bucketId + saga.Id;

            if (!_streams.TryGetValue(streamsId, out var stream))
            {
                _streams[streamsId] = stream = (await _eventStore
                    .CreateStreamAsync(saga.Id, cancellationToken)
                    .ConfigureAwait(false));
            }

            foreach (var item in headers)
            {
                stream.UncommittedHeaders[item.Key] = item.Value;
            }

            saga.GetUncommittedEvents().Cast<object>().Select(x => new EventMessage { Body = x }).ToList().ForEach(stream.Add);

            return stream;
        }

        private static async Task PersistAsync(IEventStream stream, Guid commitId, CancellationToken cancellationToken)
        {
            try
            {
                await stream.CommitChangesAsync(commitId, cancellationToken).ConfigureAwait(false);
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