using System;
using System.Threading;
using System.Threading.Tasks;

namespace NEventStore
{
    public interface IAsyncEnumerable<out T>
    {
        IAsyncEnumerator<T> GetAsyncEnumerator();
    }

    public interface IAsyncEnumerator<out T> : IAsyncDisposable
    {
        T Current { get; }

        Task<bool> MoveNextAsync();
    }

    public interface IAsyncDisposable
    {
        Task DisposeAsync();
    }

    public interface IAsyncEnumeratorSink<in T>
    {
        Task YieldAndWait(T value);
    }

    public static class AsyncEnumerableFactory
    {
        private class AsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly Func<IAsyncEnumeratorSink<T>, Task> _processor;

            public AsyncEnumerable(Func<IAsyncEnumeratorSink<T>, Task> processor)
            {
                _processor = processor;
            }

            public IAsyncEnumerator<T> GetAsyncEnumerator()
            {
                return new AsyncEnumerator<T>(_processor);
            }
        }

        private class AsyncEnumerator<T> : IAsyncEnumerator<T>
        {
            private class Sink : IAsyncEnumeratorSink<T>
            {
                private readonly AsyncEnumerator<T> _owner;

                public Sink(AsyncEnumerator<T> owner)
                {
                    _owner = owner;
                }

                public Task YieldAndWait(T value)
                {
                    return _owner.ProvideNextValue(value);
                }
            }

            private readonly Func<IAsyncEnumeratorSink<T>, Task> _processor;
            private readonly AutoResetEvent _moveNextCalled = new AutoResetEvent(false);
            private TaskCompletionSource<bool> _moveNextTask = new TaskCompletionSource<bool>();

            public AsyncEnumerator(Func<IAsyncEnumeratorSink<T>, Task> processor)
            {
                _processor = processor;
                Task.Run(() => EnumeratorThread());
            }

            public Task DisposeAsync()
            {
                return Task.CompletedTask;
            }

            public T Current { get; private set; }

            public async Task<bool> MoveNextAsync()
            {
                _moveNextCalled.Set();
                return await _moveNextTask.Task;
            }

            private async Task EnumeratorThread()
            {
                var sink = new Sink(this);
                await _processor(sink).ConfigureAwait(false);
                await TerminateEnumerator();
            }

            private async Task ProvideNextValue(T value)
            {
                while (true)
                {
                    if (_moveNextCalled.WaitOne(TimeSpan.Zero))
                    {
                        break;
                    }

                    await Task.Delay(50);
                }

                Current = value;

                var oldMoveNext = _moveNextTask;
                _moveNextTask = new TaskCompletionSource<bool>();
                oldMoveNext.SetResult(true);
            }

            private async Task TerminateEnumerator()
            {
                while (true)
                {
                    if (_moveNextCalled.WaitOne(TimeSpan.Zero))
                    {
                        break;
                    }

                    await Task.Delay(50);
                }

                Current = default(T);

                _moveNextTask.SetResult(false);
            }
        }

        public static IAsyncEnumerable<T> FromAsyncGenerator<T>(Func<IAsyncEnumeratorSink<T>, Task> processor)
        {
            return new AsyncEnumerable<T>(processor);
        }

        public static async Task ForEach<T>(IAsyncEnumerable<T> source, Func<T, Task> processor)
        {
            var enumerator = source.GetAsyncEnumerator();
            try
            {
                while (true)
                {
                    var result = await enumerator
                        .MoveNextAsync()
                        .ConfigureAwait(false);
                    if (!result)
                    {
                        break;
                    }

                    await processor(enumerator.Current)
                        .ConfigureAwait(false);
                }
            }
            finally
            {
                await enumerator
                    .DisposeAsync()
                    .ConfigureAwait(false);
            }
        }
    }
}
