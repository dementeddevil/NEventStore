#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace NEventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using FakeItEasy;
    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests.BDD;
#if MSTEST
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using FluentAssertions;
#endif
#if NUNIT
	using NUnit.Framework;	
#endif
#if XUNIT
	using Xunit;
	using Xunit.Should;
#endif

	public class PipelineHooksAwarePersistenceDecoratorTests
    {
#if MSTEST
		[TestClass]
#endif
		public class when_disposing_the_decorator : using_underlying_persistence
        {
            protected override Task Because()
            {
                Decorator.Dispose();
                return Task.CompletedTask;
            }

            [Fact]
            public void should_dispose_the_underlying_persistence()
            {
                A.CallTo(() => persistence.Dispose()).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_reading_the_all_events_from_date : using_underlying_persistence
        {
            private ICommit _commit;
            private DateTime _date;
            private IPipelineHook _hook1;
            private IPipelineHook _hook2;

            protected override Task Context()
            {
                _date = DateTime.Now;
                _commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, 0, null, null);

                _hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook1);

                _hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook2);

                A.CallTo(() => persistence.GetFromAsync(Bucket.Default, _date, A<CancellationToken>._)).Returns(Task.FromResult<IEnumerable<ICommit>>(new List<ICommit> {_commit}));
                return Task.CompletedTask;
            }

            protected override async Task Because()
            {
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                // Forces enumeration of commits.
                (await Decorator.GetFromAsync(_date, CancellationToken.None)).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => persistence.GetFromAsync(Bucket.Default, _date, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_getting_the_all_events_from_min_to_max_revision : using_underlying_persistence
        {
            private ICommit _commit;
            private DateTime _date;
            private IPipelineHook _hook1;
            private IPipelineHook _hook2;

            protected override Task Context()
            {
                _date = DateTime.Now;
                _commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, 0, null, null);

                _hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook1);

                _hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook2);

                A.CallTo(() => persistence.GetFromAsync(Bucket.Default, _commit.StreamId, 0, int.MaxValue))
                    .Returns(Task.FromResult<IEnumerable<ICommit>>(new List<ICommit> { _commit }));
                return Task.CompletedTask;
            }

            protected override async Task Because()
            {
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                // Forces enumeration of commits.
                (await Decorator.GetFromAsync(Bucket.Default, _commit.StreamId, 0, int.MaxValue)).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => persistence.GetFromAsync(Bucket.Default, _commit.StreamId, 0, int.MaxValue, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_getting_all_events_from_to : using_underlying_persistence
        {
            private ICommit _commit;
            private DateTime _end;
            private IPipelineHook _hook1;
            private IPipelineHook _hook2;
            private DateTime _start;

            protected override Task Context()
            {
                _start = DateTime.Now;
                _end = DateTime.Now;
                _commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, 0, null, null);

                _hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook1);

                _hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook2);

                A.CallTo(() => persistence.GetFromToAsync(Bucket.Default, _start, _end, A<CancellationToken>._))
                    .Returns(Task.FromResult<IEnumerable<ICommit>>(new List<ICommit> {_commit}));
                return Task.CompletedTask;
            }

            protected override async Task Because()
            {
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                // Forces enumeration of commits
                (await Decorator.GetFromToAsync(_start, _end, CancellationToken.None)).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => persistence.GetFromToAsync(Bucket.Default, _start, _end, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_committing : using_underlying_persistence
        {
            private CommitAttempt _attempt;

            protected override Task Context()
            {
                _attempt = new CommitAttempt(streamId, 1, Guid.NewGuid(), 1, DateTime.Now, null, new List<EventMessage> {new EventMessage()});
                return Task.CompletedTask;
            }

            protected override Task Because()
            {
                return Decorator.CommitAsync(_attempt, CancellationToken.None);
            }

            [Fact]
            public void should_dispose_the_underlying_persistence()
            {
                A.CallTo(() => persistence.CommitAsync(_attempt, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_reading_the_all_events_from_checkpoint : using_underlying_persistence
        {
            private ICommit _commit;
            private IPipelineHook _hook1;
            private IPipelineHook _hook2;

            protected override Task Context()
            {
                _commit = new Commit(Bucket.Default, streamId, 1, Guid.NewGuid(), 1, DateTime.Now, 0, null, null);

                _hook1 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook1);

                _hook2 = A.Fake<IPipelineHook>();
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).Returns(Task.FromResult(_commit));
                pipelineHooks.Add(_hook2);

                A.CallTo(() => persistence.GetFromAsync(0, A<CancellationToken>._))
                    .Returns(Task.FromResult<IEnumerable<ICommit>>(new List<ICommit> {_commit}));
                return Task.CompletedTask;
            }

            protected override async Task Because()
            {
                (await Decorator.GetFromAsync(0, CancellationToken.None)).ToList();
            }

            [Fact]
            public void should_call_the_underlying_persistence_to_get_events()
            {
                A.CallTo(() => persistence.GetFromAsync(0, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }

            [Fact]
            public void should_pass_all_events_through_the_pipeline_hooks()
            {
                A.CallTo(() => _hook1.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
                A.CallTo(() => _hook2.SelectAsync(_commit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_purging : using_underlying_persistence
        {
            private IPipelineHook _hook;

            protected override Task Context()
            {
                _hook = A.Fake<IPipelineHook>();
                pipelineHooks.Add(_hook);
                return Task.CompletedTask;
            }

            protected override Task Because()
            {
                return Decorator.PurgeAsync(CancellationToken.None);
            }

            [Fact]
            public void should_call_the_pipeline_hook_purge()
            {
                A.CallTo(() => _hook.OnPurgeAsync(null, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_purging_a_bucket : using_underlying_persistence
        {
            private IPipelineHook _hook;
            private const string _bucketId = "Bucket";

            protected override Task Context()
            {
                _hook = A.Fake<IPipelineHook>();
                pipelineHooks.Add(_hook);
                return Task.CompletedTask;
            }

            protected override Task Because()
            {
                return Decorator.PurgeAsync(_bucketId, CancellationToken.None);
            }

            [Fact]
            public void should_call_the_pipeline_hook_purge()
            {
                A.CallTo(() => _hook.OnPurgeAsync(_bucketId, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

#if MSTEST
		[TestClass]
#endif
		public class when_deleting_a_stream : using_underlying_persistence
        {
            private IPipelineHook _hook;
            private const string _bucketId = "Bucket";
            private const string _streamId = "Stream";

            protected override Task Context()
            {
                _hook = A.Fake<IPipelineHook>();
                pipelineHooks.Add(_hook);
                return Task.CompletedTask;
            }

            protected override Task Because()
            {
                return Decorator.DeleteStreamAsync(_bucketId, _streamId, CancellationToken.None);
            }

            [Fact]
            public void should_call_the_pipeline_hook_purge()
            {
                A.CallTo(() => _hook.OnDeleteStreamAsync(_bucketId, _streamId, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

		public abstract class using_underlying_persistence : SpecificationBase
        {
            private PipelineHooksAwarePersistanceDecorator decorator;
            protected readonly IPersistStreams persistence = A.Fake<IPersistStreams>();
            protected readonly List<IPipelineHook> pipelineHooks = new List<IPipelineHook>();
            protected readonly string streamId = Guid.NewGuid().ToString();

            public PipelineHooksAwarePersistanceDecorator Decorator
            {
                get { return decorator ?? (decorator = new PipelineHooksAwarePersistanceDecorator(persistence, pipelineHooks.Select(x => x))); }
                set { decorator = value; }
            }
        }
    }
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169