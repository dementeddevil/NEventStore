#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace NEventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
	using FluentAssertions;
	using FakeItEasy;

    using NEventStore.Persistence;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
#if MSTEST
	using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
#if NUNIT
	using NUnit.Framework;	
#endif
#if XUNIT
	using Xunit;
	using Xunit.Should;
#endif

#if MSTEST
	[TestClass]
#endif
	public class when_creating_a_new_stream : using_persistence
    {
        private IEventStream _stream;

        protected override async Task Because()
        {
            _stream = await Store.CreateStreamAsync(streamId, CancellationToken.None);
        }

        [Fact]
        public void should_return_a_new_stream()
        {
            _stream.Should().NotBeNull();
        }

        [Fact]
        public void should_return_a_stream_with_the_correct_stream_identifier()
        {
            _stream.StreamId.Should().Be(streamId);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_stream_revision()
        {
            _stream.StreamRevision.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_commit_sequence()
        {
            _stream.CommitSequence.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_no_uncommitted_events()
        {
            _stream.UncommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_no_committed_events()
        {
            _stream.CommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_empty_headers()
        {
            _stream.UncommittedHeaders.Should().BeEmpty();
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_opening_an_empty_stream_starting_at_revision_zero : using_persistence
    {
        private IEventStream _stream;

        protected override Task Context()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 0, 0, A<CancellationToken>._))
                .Returns(new ICommit[0]);
            return Task.CompletedTask;
        }

        protected override async Task Because()
        {
            _stream = await Store.OpenStreamAsync(streamId, CancellationToken.None, 0, 0);
        }

        [Fact]
        public void should_return_a_new_stream()
        {
            _stream.Should().NotBeNull();
        }

        [Fact]
        public void should_return_a_stream_with_the_correct_stream_identifier()
        {
            _stream.StreamId.Should().Be(streamId);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_stream_revision()
        {
            _stream.StreamRevision.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_a_zero_commit_sequence()
        {
            _stream.CommitSequence.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_no_uncommitted_events()
        {
            _stream.UncommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_no_committed_events()
        {
            _stream.CommittedEvents.Should().BeEmpty();
        }

        [Fact]
        public void should_return_a_stream_with_empty_headers()
        {
            _stream.UncommittedHeaders.Should().BeEmpty();
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_opening_an_empty_stream_starting_above_revision_zero : using_persistence
    {
        private const int MinRevision = 1;
        private Exception _thrown;

        protected override Task Context()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, MinRevision, int.MaxValue, A<CancellationToken>._))
                .Returns(Enumerable.Empty<ICommit>());
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Store.OpenStreamAsync(streamId, CancellationToken.None, MinRevision).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public void should_throw_a_StreamNotFoundException()
        {
            _thrown.Should().BeOfType<StreamNotFoundException>();
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_opening_a_populated_stream : using_persistence
    {
        private const int MinRevision = 17;
        private const int MaxRevision = 42;
        private ICommit _committed;
        private IEventStream _stream;

        protected override Task Context()
        {
            _committed = BuildCommitStub(MinRevision, 1);

            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, MinRevision, MaxRevision, CancellationToken.None))
                .Returns(new[] { _committed });

            var hook = A.Fake<IPipelineHook>();
            A.CallTo(() => hook.SelectAsync(_committed, A<CancellationToken>._)).Returns(_committed);
            PipelineHooks.Add(hook);
            return Task.CompletedTask;
        }

        protected override async Task Because()
        {
            _stream = await Store.OpenStreamAsync(streamId, CancellationToken.None, MinRevision, MaxRevision);
        }

        [Fact]
        public void should_invoke_the_underlying_infrastructure_with_the_values_provided()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, MinRevision, MaxRevision, CancellationToken.None)).MustHaveHappened(Repeated.Exactly.Once);
        }

        [Fact]
        public void should_provide_the_commits_to_the_selection_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.SelectAsync(_committed, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once));
        }

        [Fact]
        public void should_return_an_event_stream_containing_the_correct_stream_identifer()
        {
            _stream.StreamId.Should().Be(streamId);
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_opening_a_populated_stream_from_a_snapshot : using_persistence
    {
        private const int MaxRevision = int.MaxValue;
        private ICommit[] _committed;
        private Snapshot _snapshot;

        protected override Task Context()
        {
            _snapshot = new Snapshot(streamId, 42, "snapshot");
            _committed = new[] { BuildCommitStub(42, 0)};

            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 42, MaxRevision, A<CancellationToken>._))
                .Returns(_committed);
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            return Store.OpenStreamAsync(_snapshot, MaxRevision, CancellationToken.None);
        }

        [Fact]
        public void should_query_the_underlying_storage_using_the_revision_of_the_snapshot()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 42, MaxRevision, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_opening_a_stream_from_a_snapshot_that_is_at_the_revision_of_the_stream_head : using_persistence
    {
        private const int HeadStreamRevision = 42;
        private const int HeadCommitSequence = 15;
        private EnumerableCounter<ICommit> _committed;
        private Snapshot _snapshot;
        private IEventStream _stream;

        protected override Task Context()
        {
            _snapshot = new Snapshot(streamId, HeadStreamRevision, "snapshot");
            _committed = new EnumerableCounter<ICommit>(
                new[] { BuildCommitStub(HeadStreamRevision, HeadCommitSequence)});

            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, HeadStreamRevision, int.MaxValue, A<CancellationToken>._))
                .Returns(_committed);
            return Task.CompletedTask;
        }

        protected override async Task Because()
        {
            _stream = await Store.OpenStreamAsync(_snapshot, int.MaxValue, CancellationToken.None);
        }

        [Fact]
        public void should_return_a_stream_with_the_correct_stream_identifier()
        {
            _stream.StreamId.Should().Be(streamId);
        }

        [Fact]
        public void should_return_a_stream_with_revision_of_the_stream_head()
        {
            _stream.StreamRevision.Should().Be(HeadStreamRevision);
        }

        [Fact]
        public void should_return_a_stream_with_a_commit_sequence_of_the_stream_head()
        {
            _stream.CommitSequence.Should().Be(HeadCommitSequence);
        }

        [Fact]
        public void should_return_a_stream_with_no_committed_events()
        {
            _stream.CommittedEvents.Count.Should().Be(0);
        }

        [Fact]
        public void should_return_a_stream_with_no_uncommitted_events()
        {
            _stream.UncommittedEvents.Count.Should().Be(0);
        }

        [Fact]
        public void should_only_enumerate_the_set_of_commits_once()
        {
            _committed.GetEnumeratorCallCount.Should().Be(1);
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_reading_from_revision_zero : using_persistence
    {
        protected override Task Context()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 0, int.MaxValue, CancellationToken.None))
                .Returns(Enumerable.Empty<ICommit>());
            return Task.CompletedTask;
        }

        protected override async Task Because()
        {
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            // This forces the enumeration of the commits.
            (await Store.GetFromAsync(streamId, 0, int.MaxValue, CancellationToken.None)).ToList();
        }

        [Fact]
        public void should_pass_a_revision_range_to_the_persistence_infrastructure()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 0, int.MaxValue, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_reading_up_to_revision_revision_zero : using_persistence
    {
        private ICommit _committed;

        protected override Task Context()
        {
            _committed = BuildCommitStub(1, 1);

            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 0, int.MaxValue, A<CancellationToken>._)).Returns(new[] { _committed });
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            return Store.OpenStreamAsync(streamId, CancellationToken.None, 0, 0);
        }

        [Fact]
        public void should_pass_the_maximum_possible_revision_to_the_persistence_infrastructure()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, 0, int.MaxValue, CancellationToken.None)).MustHaveHappened(Repeated.Exactly.Once);
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_reading_from_a_null_snapshot : using_persistence
    {
        private Exception thrown;

        protected override Task Because()
        {
            thrown = Catch.Exception(() => Store.OpenStreamAsync(null, int.MaxValue, CancellationToken.None));
            return Task.CompletedTask;
        }

        [Fact]
        public void should_throw_an_ArgumentNullException()
        {
            thrown.Should().BeOfType<ArgumentNullException>();
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_reading_from_a_snapshot_up_to_revision_revision_zero : using_persistence
    {
        private ICommit _committed;
        private Snapshot snapshot;

        protected override Task Context()
        {
            snapshot = new Snapshot(streamId, 1, "snapshot");
            _committed = BuildCommitStub(1, 1);

            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, snapshot.StreamRevision, int.MaxValue, CancellationToken.None))
                .Returns(new[] { _committed });
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            return Store.OpenStreamAsync(snapshot, 0, CancellationToken.None);
        }

        [Fact]
        public void should_pass_the_maximum_possible_revision_to_the_persistence_infrastructure()
        {
            A.CallTo(() => Persistence.GetFromAsync(Bucket.Default, streamId, snapshot.StreamRevision, int.MaxValue, CancellationToken.None))
                .MustHaveHappened(Repeated.Exactly.Once);
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_committing_a_null_attempt_back_to_the_stream : using_persistence
    {
        private Exception thrown;

        protected override Task Because()
        {
            thrown = Catch.Exception(() => ((ICommitEvents) Store).CommitAsync(null, CancellationToken.None));
            return Task.CompletedTask;
        }

        [Fact]
        public void should_throw_an_ArgumentNullException()
        {
            thrown.Should().BeOfType<ArgumentNullException>();
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_committing_with_a_valid_and_populated_attempt_to_a_stream : using_persistence
    {
        private CommitAttempt _populatedAttempt;
        private ICommit _populatedCommit;

        protected override Task Context()
        {
            _populatedAttempt = BuildCommitAttemptStub(1, 1);

            A.CallTo(() => Persistence.CommitAsync(_populatedAttempt, A<CancellationToken>._))
                .ReturnsLazily((CommitAttempt attempt) =>
                {
                    _populatedCommit = new Commit(attempt.BucketId,
                        attempt.StreamId,
                        attempt.StreamRevision,
                        attempt.CommitId,
                        attempt.CommitSequence,
                        attempt.CommitStamp,
                        0,
                        attempt.Headers,
                        attempt.Events);
                    return _populatedCommit;
                });

            var hook = A.Fake<IPipelineHook>();
            A.CallTo(() => hook.PreCommitAsync(_populatedAttempt, A<CancellationToken>._)).Returns(true);

            PipelineHooks.Add(hook);
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            return ((ICommitEvents) Store).CommitAsync(_populatedAttempt, CancellationToken.None);
        }

        [Fact]
        public void should_provide_the_commit_to_the_precommit_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.PreCommitAsync(_populatedAttempt, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once));
        }

        [Fact]
        public void should_provide_the_commit_attempt_to_the_configured_persistence_mechanism()
        {
            A.CallTo(() => Persistence.CommitAsync(_populatedAttempt, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once);
        }

        [Fact]
        public void should_provide_the_commit_to_the_postcommit_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.PostCommitAsync(_populatedCommit, A<CancellationToken>._)).MustHaveHappened(Repeated.Exactly.Once));
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_a_precommit_hook_rejects_a_commit : using_persistence
    {
        private CommitAttempt _attempt;
        private ICommit _commit;

        protected override Task Context()
        {
            _attempt = BuildCommitAttemptStub(1, 1);
            _commit = BuildCommitStub(1, 1);

            var hook = A.Fake<IPipelineHook>();
            A.CallTo(() => hook.PreCommitAsync(_attempt, A<CancellationToken>._)).Returns(false);

            PipelineHooks.Add(hook);
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            return ((ICommitEvents) Store).CommitAsync(_attempt, CancellationToken.None);
        }

        [Fact]
        public void should_not_call_the_underlying_infrastructure()
        {
            A.CallTo(() => Persistence.CommitAsync(_attempt, A<CancellationToken>._)).MustNotHaveHappened();
        }

        [Fact]
        public void should_not_provide_the_commit_to_the_postcommit_hooks()
        {
            PipelineHooks.ForEach(x => A.CallTo(() => x.PostCommitAsync(_commit, A<CancellationToken>._)).MustNotHaveHappened());
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_accessing_the_underlying_persistence : using_persistence
    {
        public void should_return_a_reference_to_the_underlying_persistence_infrastructure_decorator()
        {
            Store.Advanced.Should().BeOfType<PipelineHooksAwarePersistanceDecorator>();
        }
    }

#if MSTEST
	[TestClass]
#endif
	public class when_disposing_the_event_store : using_persistence
    {
        protected override Task Because()
        {
            Store.Dispose();
            return Task.CompletedTask;
        }

        [Fact]
        public void should_dispose_the_underlying_persistence()
        {
            A.CallTo(() => Persistence.Dispose()).MustHaveHappened(Repeated.Exactly.Once);
        }
    }

    public abstract class using_persistence : SpecificationBase
    {
        private IPersistStreams persistence;

        private List<IPipelineHook> pipelineHooks;
        private OptimisticEventStore store;
        protected string streamId = Guid.NewGuid().ToString();

        protected IPersistStreams Persistence
        {
            get { return persistence ?? (persistence = A.Fake<IPersistStreams>()); }
        }

        protected List<IPipelineHook> PipelineHooks
        {
            get { return pipelineHooks ?? (pipelineHooks = new List<IPipelineHook>()); }
        }

        protected OptimisticEventStore Store
        {
            get { return store ?? (store = new OptimisticEventStore(Persistence, PipelineHooks.Select(x => x))); }
        }

        protected override Task Cleanup()
        {
            streamId = Guid.NewGuid().ToString();
            return Task.CompletedTask;
        }

        protected CommitAttempt BuildCommitAttemptStub(Guid commitId)
        {
            return new CommitAttempt(Bucket.Default, streamId, 1, commitId, 1, SystemTime.UtcNow, null, null);
        }

        protected ICommit BuildCommitStub(int streamRevision, int commitSequence)
        {
            List<EventMessage> events = new[] {new EventMessage()}.ToList();
            return new Commit(Bucket.Default, streamId, streamRevision, Guid.NewGuid(), commitSequence, SystemTime.UtcNow, 0, null, events);
        }

        protected CommitAttempt BuildCommitAttemptStub(int streamRevision, int commitSequence)
        {
            List<EventMessage> events = new[] { new EventMessage() }.ToList();
            return new CommitAttempt(Bucket.Default, streamId, streamRevision, Guid.NewGuid(), commitSequence, SystemTime.UtcNow, null, events);
        }

        protected ICommit BuildCommitStub(Guid commitId, int streamRevision, int commitSequence)
        {
            List<EventMessage> events = new[] {new EventMessage()}.ToList();
            return new Commit(Bucket.Default, streamId, streamRevision, commitId, commitSequence, SystemTime.UtcNow,0, null, events);
        }
    }
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169