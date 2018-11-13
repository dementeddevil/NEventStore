#pragma warning disable 169
// ReSharper disable InconsistentNaming

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NEventStore.Diagnostics;
using NEventStore.Persistence.AcceptanceTests.BDD;
using Xunit;

namespace NEventStore.Persistence.AcceptanceTests
{
    public class when_a_commit_header_has_a_name_that_contains_a_period : PersistenceEngineConcern
    {
        private ICommit _persisted;
        private string _streamId;

        public when_a_commit_header_has_a_name_that_contains_a_period(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            var attempt = new CommitAttempt(_streamId,
                2,
                Guid.NewGuid(),
                1,
                DateTime.Now,
                new Dictionary<string, object> {{"key.1", "value"}},
                new List<EventMessage> {new EventMessage {Body = new ExtensionMethods.SomeDomainEvent {SomeProperty = "Test"}}});
            return Persistence.CommitAsync(attempt, CancellationToken.None);
        }

        protected override async Task Because()
        {
            _persisted = (await Persistence.GetFromAsync(_streamId, 0, int.MaxValue, CancellationToken.None)).First();
        }

        [Fact]
        public Task should_correctly_deserialize_headers()
        {
            return Execute(
                async () =>
                {
                    _persisted.Headers.Keys.Should().Contain("key.1");
                });
        }
    }

    public class when_a_commit_is_successfully_persisted : PersistenceEngineConcern
    {
        private CommitAttempt _attempt;
        private DateTime _now;
        private ICommit _persisted;
        private string _streamId;

        public when_a_commit_is_successfully_persisted(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Context()
        {
            _now = SystemTime.UtcNow.AddYears(1);
            _streamId = Guid.NewGuid().ToString();
            _attempt = _streamId.BuildAttempt(_now);

            return Persistence.CommitAsync(_attempt, CancellationToken.None);
        }

        protected override async Task Because()
        {
            _persisted = (await Persistence.GetFromAsync(_streamId, 0, int.MaxValue, CancellationToken.None)).First();
        }

        [Fact]
        public Task should_correctly_persist_the_stream_identifier()
        {
            return Execute(
                async () =>
                {
                    _persisted.StreamId.Should().Be(_attempt.StreamId);
                });
        }

        [Fact]
        public Task should_correctly_persist_the_stream_stream_revision()
        {
            return Execute(
                async () =>
                {
                    _persisted.StreamRevision.Should().Be(_attempt.StreamRevision);
                });
        }

        [Fact]
        public Task should_correctly_persist_the_commit_identifier()
        {
            return Execute(
                async () =>
                {
                    _persisted.CommitId.Should().Be(_attempt.CommitId);
                });
        }

        [Fact]
        public Task should_correctly_persist_the_commit_sequence()
        {
            return Execute(
                async () =>
                {
                    _persisted.CommitSequence.Should().Be(_attempt.CommitSequence);
                });
        }

        // persistence engines have varying levels of precision with respect to time.
        [Fact]
        public Task should_correctly_persist_the_commit_stamp()
        {
            return Execute(
                async () =>
                {
                    var difference = _persisted.CommitStamp.Subtract(_now);
                    difference.Days.Should().Be(0);
                    difference.Hours.Should().Be(0);
                    difference.Minutes.Should().Be(0);
                    difference.Should().BeLessOrEqualTo(TimeSpan.FromSeconds(1));
                });
        }

        [Fact]
        public Task should_correctly_persist_the_headers()
        {
            return Execute(
                async () =>
                {
                    _persisted.Headers.Count.Should().Be(_attempt.Headers.Count);
                });
        }

        [Fact]
        public Task should_correctly_persist_the_events()
        {
            return Execute(
                async () =>
                {
                    _persisted.Events.Count.Should().Be(_attempt.Events.Count);
                });
        }

        [Fact]
        public Task should_add_the_commit_to_the_set_of_undispatched_commits()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetUndispatchedCommitsAsync(CancellationToken.None))
                        .FirstOrDefault(x => x.CommitId == _attempt.CommitId)
                        .Should().NotBeNull();
                });
        }

        [Fact]
        public Task should_cause_the_stream_to_be_found_in_the_list_of_streams_to_snapshot()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(1, CancellationToken.None))
                        .FirstOrDefault(x => x.StreamId == _streamId)
                        .Should().NotBeNull();
                });
        }
    }

    public class when_reading_from_a_given_revision : PersistenceEngineConcern
    {
        private const int LoadFromCommitContainingRevision = 3;
        private const int UpToCommitWithContainingRevision = 5;
        private ICommit[] _committed;
        private ICommit _oldest, _oldest2, _oldest3;
        private string _streamId;

        public when_reading_from_a_given_revision(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _oldest = await Persistence.CommitSingleAsync(); // 2 events, revision 1-2
            _oldest2 = await Persistence.CommitNextAsync(_oldest); // 2 events, revision 3-4
            _oldest3 = await Persistence.CommitNextAsync(_oldest2); // 2 events, revision 5-6
            await Persistence.CommitNextAsync(_oldest3); // 2 events, revision 7-8

            _streamId = _oldest.StreamId;
        }

        protected override async Task Because()
        {
            _committed = (await Persistence.GetFromAsync(_streamId, LoadFromCommitContainingRevision, UpToCommitWithContainingRevision, CancellationToken.None)).ToArray();
        }

        [Fact]
        public Task should_start_from_the_commit_which_contains_the_min_stream_revision_specified()
        {
            return Execute(
                async () =>
                {
                    _committed.First().CommitId.Should().Be(_oldest2.CommitId); // contains revision 3
                });
        }

        [Fact]
        public Task should_read_up_to_the_commit_which_contains_the_max_stream_revision_specified()
        {
            return Execute(
                async () =>
                {
                    _committed.Last().CommitId.Should().Be(_oldest3.CommitId); // contains revision 5
                });
        }
    }

    public class when_reading_from_a_given_revision_to_commit_revision : PersistenceEngineConcern
    {
        private const int LoadFromCommitContainingRevision = 3;
        private const int UpToCommitWithContainingRevision = 6;
        private ICommit[] _committed;
        private ICommit _oldest, _oldest2, _oldest3;
        private string _streamId;

        public when_reading_from_a_given_revision_to_commit_revision(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _oldest = await Persistence.CommitSingleAsync(); // 2 events, revision 1-2
            _oldest2 = await Persistence.CommitNextAsync(_oldest); // 2 events, revision 3-4
            _oldest3 = await Persistence.CommitNextAsync(_oldest2); // 2 events, revision 5-6
            await Persistence.CommitNextAsync(_oldest3); // 2 events, revision 7-8

            _streamId = _oldest.StreamId;
        }

        protected override async Task Because()
        {
            _committed = (await Persistence
                    .GetFromAsync(_streamId, LoadFromCommitContainingRevision, UpToCommitWithContainingRevision, CancellationToken.None))
                .ToArray();
        }

        [Fact]
        public Task should_start_from_the_commit_which_contains_the_min_stream_revision_specified()
        {
            return Execute(
                async () =>
                {
                    _committed.First().CommitId.Should().Be(_oldest2.CommitId); // contains revision 3
                });
        }

        [Fact]
        public Task should_read_up_to_the_commit_which_contains_the_max_stream_revision_specified()
        {
            return Execute(
                async () =>
                {
                    _committed.Last().CommitId.Should().Be(_oldest3.CommitId); // contains revision 6
                });
        }
    }

    public class when_committing_a_stream_with_the_same_revision : PersistenceEngineConcern
    {
        private CommitAttempt _attemptWithSameRevision;
        private Exception _thrown;

        public when_committing_a_stream_with_the_same_revision(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            var commit = await Persistence.CommitSingleAsync();
            _attemptWithSameRevision = commit.StreamId.BuildAttempt();
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitAsync(_attemptWithSameRevision).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_throw_a_ConcurrencyException()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeOfType<ConcurrencyException>();
                });
        }
    }

    //TODO:This test looks exactly like the one above. What are we trying to prove?
    public class when_committing_a_stream_with_the_same_sequence : PersistenceEngineConcern
    {
        private CommitAttempt _attempt1, _attempt2;
        private Exception _thrown;

        public when_committing_a_stream_with_the_same_sequence(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Context()
        {
            var streamId = Guid.NewGuid().ToString();
            _attempt1 = streamId.BuildAttempt();
            _attempt2 = streamId.BuildAttempt(); //TODO mutate a bit

            return Persistence.CommitAsync(_attempt1);
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitAsync(_attempt2).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_throw_a_ConcurrencyException()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeOfType<ConcurrencyException>();
                });
        }
    }

    //TODO:This test looks exactly like the one above. What are we trying to prove?
    public class when_attempting_to_overwrite_a_committed_sequence : PersistenceEngineConcern
    {
        private CommitAttempt _failedAttempt;
        private Exception _thrown;

        public when_attempting_to_overwrite_a_committed_sequence(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            var streamId = Guid.NewGuid().ToString();
            var successfulAttempt = streamId.BuildAttempt();
            await Persistence.CommitAsync(successfulAttempt);
            _failedAttempt = streamId.BuildAttempt();
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitAsync(_failedAttempt).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_throw_a_ConcurrencyException()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeOfType<ConcurrencyException>();
                });
        }
    }

    public class when_attempting_to_persist_a_commit_twice : PersistenceEngineConcern
    {
        private CommitAttempt _attemptTwice;
        private Exception _thrown;

        public when_attempting_to_persist_a_commit_twice(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            var commit = await Persistence.CommitSingleAsync();
            _attemptTwice = new CommitAttempt(
                commit.BucketId,
                commit.StreamId,
                commit.StreamRevision,
                commit.CommitId,
                commit.CommitSequence,
                commit.CommitStamp,
                commit.Headers,
                commit.Events);
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitAsync(_attemptTwice).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_throw_a_DuplicateCommitException()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeOfType<DuplicateCommitException>();
                });
        }
    }

    public class when_a_commit_has_been_marked_as_dispatched : PersistenceEngineConcern
    {
        private ICommit _commit;

        public when_a_commit_has_been_marked_as_dispatched(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _commit = await Persistence.CommitSingleAsync();
        }

        protected override Task Because()
        {
            return Persistence.MarkCommitAsDispatchedAsync(_commit, CancellationToken.None);
        }

        [Fact]
        public Task should_no_longer_be_found_in_the_set_of_undispatched_commits()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetUndispatchedCommitsAsync(CancellationToken.None))
                        .FirstOrDefault(x => x.CommitId == _commit.CommitId).Should().BeNull();
                });
        }
    }

    public class when_committing_more_events_than_the_configured_page_size : PersistenceEngineConcern
    {
        private CommitAttempt[] _committed;
        private ICommit[] _loaded;
        private string _streamId;

        public when_committing_more_events_than_the_configured_page_size(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _committed = (await Persistence.CommitManyAsync(ConfiguredPageSizeForTesting + 2, _streamId)).ToArray();
        }

        protected override async Task Because()
        {
            _loaded = (await Persistence.GetFromAsync(_streamId, 0, int.MaxValue, CancellationToken.None))
                .ToArray();
        }

        [Fact]
        public Task should_load_the_same_number_of_commits_which_have_been_persisted()
        {
            return Execute(
                async () =>
                {
                    _loaded.Length.Should().Be(_committed.Length);
                });
        }

        [Fact]
        public Task should_load_the_same_commits_which_have_been_persisted()
        {
            return Execute(
                async () =>
                {
                    _committed
                        .All(commit => _loaded.SingleOrDefault(loaded => loaded.CommitId == commit.CommitId) != null)
                        .Should().BeTrue();
                });
        }
    }

    public class when_saving_a_snapshot : PersistenceEngineConcern
    {
        private bool _added;
        private Snapshot _snapshot;
        private string _streamId;

        public when_saving_a_snapshot(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _snapshot = new Snapshot(_streamId, 1, "Snapshot");
            return Persistence.CommitSingleAsync(_streamId);
        }

        protected override async Task Because()
        {
            _added = await Persistence.AddSnapshotAsync(_snapshot, CancellationToken.None);
        }

        [Fact]
        public Task should_indicate_the_snapshot_was_added()
        {
            return Execute(
                async () =>
                {
                    _added.Should().BeTrue();
                });
        }

        [Fact]
        public Task should_be_able_to_retrieve_the_snapshot()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetSnapshotAsync(_streamId, _snapshot.StreamRevision, CancellationToken.None))
                        .Should().NotBeNull();
                });
        }
    }

    public class when_retrieving_a_snapshot : PersistenceEngineConcern
    {
        private ISnapshot _correct;
        private ISnapshot _snapshot;
        private string _streamId;
        private ISnapshot _tooFarForward;

        public when_retrieving_a_snapshot(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            var commit1 = await Persistence.CommitSingleAsync(_streamId); // rev 1-2
            var commit2 = await Persistence.CommitNextAsync(commit1); // rev 3-4
            await Persistence.CommitNextAsync(commit2); // rev 5-6

            await Persistence.AddSnapshotAsync(new Snapshot(_streamId, 1, string.Empty), CancellationToken.None); //Too far back
            await Persistence.AddSnapshotAsync(_correct = new Snapshot(_streamId, 3, "Snapshot"), CancellationToken.None);
            await Persistence.AddSnapshotAsync(_tooFarForward = new Snapshot(_streamId, 5, string.Empty), CancellationToken.None);
        }

        protected override async Task Because()
        {
            _snapshot = await Persistence.GetSnapshotAsync(_streamId, _tooFarForward.StreamRevision - 1, CancellationToken.None);
        }

        [Fact]
        public Task should_load_the_most_recent_prior_snapshot()
        {
            return Execute(
                async () =>
                {
                    _snapshot.StreamRevision.Should().Be(_correct.StreamRevision);
                });
        }

        [Fact]
        public Task should_have_the_correct_snapshot_payload()
        {
            return Execute(
                async () =>
                {
                    _snapshot.Payload.Should().Be(_correct.Payload);
                });
        }
    }

    public class when_a_snapshot_has_been_added_to_the_most_recent_commit_of_a_stream : PersistenceEngineConcern
    {
        private const string SnapshotData = "snapshot";
        private ICommit _newest;
        private ICommit _oldest, _oldest2;
        private string _streamId;

        public when_a_snapshot_has_been_added_to_the_most_recent_commit_of_a_stream(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _oldest = await Persistence.CommitSingleAsync(_streamId);
            _oldest2 = await Persistence.CommitNextAsync(_oldest);
            _newest = await Persistence.CommitNextAsync(_oldest2);
        }

        protected override Task Because()
        {
            return Persistence.AddSnapshotAsync(new Snapshot(_streamId, _newest.StreamRevision, SnapshotData), CancellationToken.None);
        }

        [Fact]
        public Task should_no_longer_find_the_stream_in_the_set_of_streams_to_be_snapshot()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(1, CancellationToken.None))
                        .Any(x => x.StreamId == _streamId).Should().BeFalse();
                });
        }
    }

    public class when_adding_a_commit_after_a_snapshot : PersistenceEngineConcern
    {
        private const int WithinThreshold = 2;
        private const int OverThreshold = 3;
        private const string SnapshotData = "snapshot";
        private ICommit _oldest, _oldest2;
        private string _streamId;

        public when_adding_a_commit_after_a_snapshot(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _oldest = await Persistence.CommitSingleAsync(_streamId);
            _oldest2 = await Persistence.CommitNextAsync(_oldest);
            await Persistence.AddSnapshotAsync(new Snapshot(_streamId, _oldest2.StreamRevision, SnapshotData), CancellationToken.None);
        }

        protected override Task Because()
        {
            return Persistence.CommitAsync(_oldest2.BuildNextAttempt(), CancellationToken.None);
        }

        // Because Raven and Mongo update the stream head asynchronously, occasionally will fail this test
        [Fact]
        public Task should_find_the_stream_in_the_set_of_streams_to_be_snapshot_when_within_the_threshold()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(WithinThreshold, CancellationToken.None))
                        .FirstOrDefault(x => x.StreamId == _streamId).Should().NotBeNull();
                });
        }

        [Fact]
        public Task should_not_find_the_stream_in_the_set_of_streams_to_be_snapshot_when_over_the_threshold()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(OverThreshold, CancellationToken.None))
                        .Any(x => x.StreamId == _streamId).Should().BeFalse();
                });
        }
    }

    public class when_reading_all_commits_from_a_particular_point_in_time : PersistenceEngineConcern
    {
        private ICommit[] _committed;
        private CommitAttempt _first;
        private DateTime _now;
        private ICommit _second;
        private string _streamId;
        private ICommit _third;

        public when_reading_all_commits_from_a_particular_point_in_time(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();

            _now = SystemTime.UtcNow.AddYears(1);
            _first = _streamId.BuildAttempt(_now.AddSeconds(1));
            await Persistence.CommitAsync(_first);

            _second = await Persistence.CommitNextAsync(_first);
            _third = await Persistence.CommitNextAsync(_second);
            await Persistence.CommitNextAsync(_third);
        }

        protected override async Task Because()
        {
            _committed = (await Persistence.GetFromAsync(_now, CancellationToken.None))
                .ToArray();
        }

        [Fact]
        public Task should_return_all_commits_on_or_after_the_point_in_time_specified()
        {
            return Execute(
                async () =>
                {
                    _committed.Length.Should().Be(4);
                });
        }
    }

    public class when_paging_over_all_commits_from_a_particular_point_in_time : PersistenceEngineConcern
    {
        private CommitAttempt[] _committed;
        private ICommit[] _loaded;
        private DateTime _start;
        private Guid _streamId;

        public when_paging_over_all_commits_from_a_particular_point_in_time(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _start = SystemTime.UtcNow;
            // Due to loss in precision in various storage engines, we're rounding down to the
            // nearest second to ensure include all commits from the 'start'.
            _start = _start.AddSeconds(-1); 
            _committed = (await Persistence.CommitManyAsync(ConfiguredPageSizeForTesting + 2))
                .ToArray();
        }

        protected override async Task Because()
        {
            _loaded = (await Persistence.GetFromAsync(_start, CancellationToken.None))
                .ToArray();
        }

        [Fact]
        public Task should_load_the_same_number_of_commits_which_have_been_persisted()
        {
            return Execute(
                async () =>
                {
                    _loaded.Length.Should().Be(_committed.Length);
                });
        }

        [Fact]
        public Task should_load_the_same_commits_which_have_been_persisted()
        {
            return Execute(
                async () =>
                {
                    _committed
                        .All(commit => _loaded.SingleOrDefault(loaded => loaded.CommitId == commit.CommitId) != null)
                        .Should().BeTrue();
                });
        }
    }

    public class when_paging_over_all_commits_from_a_particular_checkpoint : PersistenceEngineConcern
    {
        private List<Guid> _committed;
        private ICollection<Guid> _loaded;
        private Guid _streamId;
        private const int checkPoint = 2;

        public when_paging_over_all_commits_from_a_particular_checkpoint(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _committed = (await Persistence.CommitManyAsync(ConfiguredPageSizeForTesting + 1))
                .Select(c => c.CommitId).ToList();
        }

        protected override async Task Because()
        {
            _loaded = (await Persistence.GetFromAsync(CancellationToken.None, checkPoint.ToString()))
                .Select(c => c.CommitId).ToList();
        }

        [Fact]
        public Task should_load_the_same_number_of_commits_which_have_been_persisted_starting_from_the_checkpoint()
        {
            return Execute(
                async () =>
                {
                    _loaded.Count.Should().Be(_committed.Count - checkPoint);
                });
        }

        [Fact]
        public Task should_load_only_the_commits_starting_from_the_checkpoint()
        {
            return Execute(
                async () =>
                {
                    _committed.Skip(checkPoint).All(x => _loaded.Contains(x)).Should().BeTrue(); // all commits should be found in loaded collection
                });
        }
    }

    public class when_reading_all_commits_from_the_year_1_AD : PersistenceEngineConcern
    {
        private Exception _thrown;

        public when_reading_all_commits_from_the_year_1_AD(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Because()
        {
// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            _thrown = Catch.Exception(() => Persistence.GetFromAsync(DateTime.MinValue, CancellationToken.None).GetAwaiter().GetResult().FirstOrDefault());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_NOT_throw_an_exception()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeNull();
                });
        }
    }

    public class when_purging_all_commits : PersistenceEngineConcern
    {
        public when_purging_all_commits(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Context()
        {
            return Persistence.CommitSingleAsync();
        }

        protected override Task Because()
        {
            return Persistence.PurgeAsync(CancellationToken.None);
        }

        [Fact]
        public Task should_not_find_any_commits_stored()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetFromAsync(DateTime.MinValue, CancellationToken.None))
                        .Count().Should().Be(0);
                });
        }

        [Fact]
        public Task should_not_find_any_streams_to_snapshot()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(0, CancellationToken.None))
                        .Count().Should().Be(0);
                });
        }

        [Fact]
        public Task should_not_find_any_undispatched_commits()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetUndispatchedCommitsAsync(CancellationToken.None))
                        .Count().Should().Be(0);
                });
        }
    }

    public class when_invoking_after_disposal : PersistenceEngineConcern
    {
        private Exception _thrown;

        public when_invoking_after_disposal(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override Task Context()
        {
            Persistence.Dispose();
            return Task.CompletedTask;
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitSingleAsync().GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_throw_an_ObjectDisposedException()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeOfType<ObjectDisposedException>();
                });
        }
    }

    public class when_committing_a_stream_with_the_same_id_as_a_stream_in_another_bucket : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";
        private string _streamId;
        private static CommitAttempt _attemptForBucketB;
        private static Exception _thrown;
        private DateTime _attemptACommitStamp;

        public when_committing_a_stream_with_the_same_id_as_a_stream_in_another_bucket(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            var now = SystemTime.UtcNow;
            await Persistence.CommitAsync(_streamId.BuildAttempt(now, _bucketAId));
            _attemptACommitStamp = (await Persistence.GetFromAsync(_bucketAId, _streamId, 0, int.MaxValue, CancellationToken.None)).First().CommitStamp;
            _attemptForBucketB = _streamId.BuildAttempt(now.Subtract(TimeSpan.FromDays(1)),_bucketBId);
        }

        protected override Task Because()
        {
            _thrown = Catch.Exception(() => Persistence.CommitAsync(_attemptForBucketB).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }

        [Fact]
        public Task should_succeed()
        {
            return Execute(
                async () =>
                {
                    _thrown.Should().BeNull();
                });
        }

        [Fact]
        public Task should_persist_to_the_correct_bucket()
        {
            return Execute(
                async () =>
                {
                    var stream = (await Persistence.GetFromAsync(_bucketBId, _streamId, 0, int.MaxValue, CancellationToken.None))
                        .ToArray();
                    stream.Should().NotBeNull();
                    stream.Count().Should().Be(1);
                });
        }

        [Fact]
        public Task should_not_affect_the_stream_from_the_other_bucket()
        {
            return Execute(
                async () =>
                {
                    var stream = (await Persistence.GetFromAsync(_bucketAId, _streamId, 0, int.MaxValue, CancellationToken.None))
                        .ToArray();
                    stream.Should().NotBeNull();
                    stream.Count().Should().Be(1);
                    stream.First().CommitStamp.Should().Be(_attemptACommitStamp);
                });
        }
    }

    public class when_saving_a_snapshot_for_a_stream_with_the_same_id_as_a_stream_in_another_bucket : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        string _streamId;
        
        private static Snapshot _snapshot;

        public when_saving_a_snapshot_for_a_stream_with_the_same_id_as_a_stream_in_another_bucket(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            _snapshot = new Snapshot(_bucketBId, _streamId, 1, "Snapshot");
            await Persistence.CommitAsync(_streamId.BuildAttempt(bucketId: _bucketAId));
            await Persistence.CommitAsync(_streamId.BuildAttempt(bucketId: _bucketBId));
        }

        protected override Task Because()
        {
            return Persistence.AddSnapshotAsync(_snapshot, CancellationToken.None);
        }

        [Fact]
        public Task should_affect_snapshots_from_another_bucket()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetSnapshotAsync(_bucketAId, _streamId, _snapshot.StreamRevision, CancellationToken.None))
                        .Should().BeNull();
                });
        }
    }

    public class when_reading_all_commits_from_a_particular_point_in_time_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        private static DateTime _now;
        private static ICommit[] _returnedCommits;
        private CommitAttempt _commitToBucketB;

        public when_reading_all_commits_from_a_particular_point_in_time_and_there_are_streams_in_multiple_buckets(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _now = SystemTime.UtcNow.AddYears(1);

            var commitToBucketA = Guid.NewGuid().ToString().BuildAttempt(_now.AddSeconds(1), _bucketAId);

            await Persistence.CommitAsync(commitToBucketA, CancellationToken.None);
            await Persistence.CommitAsync(commitToBucketA = commitToBucketA.BuildNextAttempt(), CancellationToken.None);
            await Persistence.CommitAsync(commitToBucketA = commitToBucketA.BuildNextAttempt(), CancellationToken.None);
            await Persistence.CommitAsync(commitToBucketA.BuildNextAttempt(), CancellationToken.None);

            _commitToBucketB = Guid.NewGuid().ToString().BuildAttempt(_now.AddSeconds(1), _bucketBId);

            await Persistence.CommitAsync(_commitToBucketB, CancellationToken.None);
        }

        protected override async Task Because()
        {
            _returnedCommits = (await Persistence.GetFromAsync(_bucketAId, _now, CancellationToken.None)).ToArray();
        }
        
        [Fact]
        public Task should_not_return_commits_from_other_buckets()
        {
            return Execute(
                async () =>
                {
                    _returnedCommits.Any(c => c.CommitId.Equals(_commitToBucketB.CommitId)).Should().BeFalse();
                });
        }
    }

    public class when_getting_all_commits_since_checkpoint_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        private ICommit[] _commits;

        public when_getting_all_commits_since_checkpoint_and_there_are_streams_in_multiple_buckets(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            const string bucketAId = "a";
            const string bucketBId = "b";
            await Persistence
                .CommitAsync(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketAId), CancellationToken.None)
                .ConfigureAwait(false);
            await Persistence
                .CommitAsync(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketBId), CancellationToken.None)
                .ConfigureAwait(false);
            await Persistence
                .CommitAsync(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketAId), CancellationToken.None)
                .ConfigureAwait(false);
        }

        protected override async Task Because()
        {
            _commits = (await Persistence
                    .GetFromStartAsync(CancellationToken.None)
                    .ConfigureAwait(false))
                .ToArray();
        }

        [Fact]
        public void should_not_be_empty()
        {
            _commits.Should().NotBeEmpty();
        }

        [Fact]
        public async Task should_be_in_order_by_checkpoint()
        {
            var checkpoint = await Persistence
                .GetCheckpointAsync(CancellationToken.None)
                .ConfigureAwait(false);

            foreach (var commit in _commits)
            {
                var commitCheckpoint = await Persistence
                    .GetCheckpointAsync(CancellationToken.None, commit.CheckpointToken)
                    .ConfigureAwait(false);

                commitCheckpoint.Should().BeGreaterThan(checkpoint);

                checkpoint = await Persistence
                    .GetCheckpointAsync(CancellationToken.None, commit.CheckpointToken)
                    .ConfigureAwait(false);
            }
        }
    }

    public class when_purging_all_commits_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        const string _bucketAId = "a";
        const string _bucketBId = "b";

        string _streamId;

        public when_purging_all_commits_and_there_are_streams_in_multiple_buckets(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            await Persistence.CommitAsync(_streamId.BuildAttempt(bucketId: _bucketAId), CancellationToken.None);
            await Persistence.CommitAsync(_streamId.BuildAttempt(bucketId: _bucketBId), CancellationToken.None);
        }

        protected override Task Because()
        {
            return Persistence.PurgeAsync(CancellationToken.None);
        }

        [Fact]
        public Task should_purge_all_commits_stored_in_bucket_a()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetFromAsync(_bucketAId, DateTime.MinValue, CancellationToken.None)).Count().Should().Be(0);
                });
        }

        [Fact]
        public Task should_purge_all_commits_stored_in_bucket_b()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetFromAsync(_bucketBId, DateTime.MinValue, CancellationToken.None)).Count().Should()
                        .Be(0);
                });
        }

        [Fact]
        public Task should_purge_all_streams_to_snapshot_in_bucket_a()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(_bucketAId, 0, CancellationToken.None)).Count()
                        .Should().Be(0);
                });
        }

        [Fact]
        public Task should_purge_all_streams_to_snapshot_in_bucket_b()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetStreamsToSnapshotAsync(_bucketBId, 0, CancellationToken.None)).Count()
                        .Should().Be(0);
                });
        }

        [Fact]
        public Task should_purge_all_undispatched_commits()
        {
            return Execute(
                async () =>
                {
                    (await Persistence.GetUndispatchedCommitsAsync(CancellationToken.None)).Count().Should().Be(0);
                });
        }
    }

    public class when_gettingfromcheckpoint_amount_of_commits_exceeds_pagesize : PersistenceEngineConcern
    {
        private ICommit[] _commits;
        private int _moreThanPageSize;

        public when_gettingfromcheckpoint_amount_of_commits_exceeds_pagesize(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        protected override async Task Because()
        {
            _moreThanPageSize = ConfiguredPageSizeForTesting + 1;

            var eventStore = new OptimisticEventStore(Persistence, null);
            // TODO: Not sure how to set the actual pagesize to the const defined above
            for (var i = 0; i < _moreThanPageSize; i++)
            {
                using (var stream = await eventStore.OpenStreamAsync(Guid.NewGuid(), CancellationToken.None))
                {
                    stream.Add(new EventMessage { Body = i });
                    await stream.CommitChangesAsync(Guid.NewGuid(), CancellationToken.None);
                }
            }

            var commits = (await Persistence.GetFromAsync(DateTime.MinValue, CancellationToken.None)).ToArray();
            _commits = (await Persistence.GetFromAsync(CancellationToken.None)).ToArray();
        }

        [Fact]
        public Task Should_have_expected_number_of_commits()
        {
            return Execute(
                async () =>
                {
                    _commits.Length.Should().Be(_moreThanPageSize);
                });
        }
    }
    
    /* Commented out because it's not a scenario we're supporting
     * public class TransactionConcern : SpecificationBase, IUseFixture<PersistenceEngineFixture>
    {
        private ICommit[] _commits;
        private PersistenceEngineFixture _fixture;
        private const int Loop = 2;
        private const int StreamsPerTransaction = 20;

        protected override Task Because()
        {
            Parallel.For(0, Loop, i =>
            {
                var eventStore = new OptimisticEventStore(_fixture.Persistence, null);
                using (var scope = new TransactionScope(TransactionScopeOption.Required,
                    new TransactionOptions {IsolationLevel = IsolationLevel.Serializable}))
                {
                    int j;
                    for (j = 0; j < StreamsPerTransaction; j++)
                    {
                        using (var stream = eventStore.OpenStream(i.ToString() + "-" + j.ToString()))
                        {
                            for (int k = 0; k < 10; k++)
                            {
                                stream.Add(new EventMessage {Body = "body" + k});
                            }
                            stream.CommitChanges(Guid.NewGuid());
                        }
                    }
                    scope.Complete();
                }
            });
            _commits = _fixture.Persistence.GetFrom(null).ToArray();
        }

        [Fact]
        public void Should_have_expected_number_of_commits()
        {
            _commits.Length.Should().Be(Loop * StreamsPerTransaction);
        }

        /* [Fact]
        public void ScopeCompleteAndSerializable()
        {
            int loop = 10;
            using (var scope = new TransactionScope(
                TransactionScopeOption.Required,
                new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.Serializable
                }))
            {
                Parallel.For(0, loop, i =>
                {
                    Console.WriteLine("Creating stream {0} on thread {1}", i, Thread.CurrentThread.ManagedThreadId);
                    var eventStore = new OptimisticEventStore(_fixture.Persistence, null);
                    string streamId = i.ToString(CultureInfo.InvariantCulture);
                    using (var stream = eventStore.OpenStream(streamId))
                    {
                        stream.Add(new EventMessage { Body = "body1" });
                        stream.Add(new EventMessage { Body = "body2" });
                        stream.CommitChanges(Guid.NewGuid());
                    }
                });
                scope.Complete();
            }
            ICheckpoint checkpoint = _fixture.Persistence.GetCheckpoint();
            ICommit[] commits = _fixture.Persistence.GetFrom(checkpoint.Value).ToArray();
            commits.Length.Should().Be(loop);
        }

        [Fact]
        public void ScopeNotCompleteAndReadCommitted()
        {
            int loop = 10;
            using (var scope = new TransactionScope(
                TransactionScopeOption.Required,
                new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.ReadCommitted
                }))
            {
                Parallel.For(0, loop, i =>
                {
                    Console.WriteLine("Creating stream {0} on thread {1}", i, Thread.CurrentThread.ManagedThreadId);
                    var eventStore = new OptimisticEventStore(_fixture.Persistence, null);
                    string streamId = i.ToString(CultureInfo.InvariantCulture);
                    using (var stream = eventStore.OpenStream(streamId))
                    {
                        stream.Add(new EventMessage { Body = "body1" });
                        stream.Add(new EventMessage { Body = "body2" });
                        stream.CommitChanges(Guid.NewGuid());
                    }
                });
            }
            ICheckpoint checkpoint = _fixture.Persistence.GetCheckpoint();
            ICommit[] commits = _fixture.Persistence.GetFrom(checkpoint.Value).ToArray();
            commits.Length.Should().Be(0);
        }

        [Fact]
        public void ScopeNotCompleteAndSerializable()
        {
            int loop = 10;
            using (var scope = new TransactionScope(
                TransactionScopeOption.Required,
                new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.ReadCommitted
                }))
            {
                Parallel.For(0, loop, i =>
                {
                    Console.WriteLine("Creating stream {0} on thread {1}", i, Thread.CurrentThread.ManagedThreadId);
                    var eventStore = new OptimisticEventStore(_fixture.Persistence, null);
                    string streamId = i.ToString(CultureInfo.InvariantCulture);
                    using (var stream = eventStore.OpenStream(streamId))
                    {
                        stream.Add(new EventMessage { Body = "body1" });
                        stream.Add(new EventMessage { Body = "body2" });
                        stream.CommitChanges(Guid.NewGuid());
                    }
                });
            }
            ICheckpoint checkpoint = _fixture.Persistence.GetCheckpoint();
            ICommit[] commits = _fixture.Persistence.GetFrom(checkpoint.Value).ToArray();
            commits.Length.Should().Be(0);
        }#1#

        public void SetFixture(PersistenceEngineFixture data)
        {
            _fixture = data;
        }
    }*/

    public class when_a_payload_is_large : PersistenceEngineConcern
    {
        public when_a_payload_is_large(PersistenceEngineFixture fixtureData) : base(fixtureData)
        { }

        [Fact]
        public Task can_commit()
        {
            return Execute(
                async () =>
                {
                    const int bodyLength = 100000;
                    var attempt = new CommitAttempt(
                        Bucket.Default,
                        Guid.NewGuid().ToString(),
                        1,
                        Guid.NewGuid(),
                        1,
                        DateTime.UtcNow,
                        new Dictionary<string, object>(),
                        new List<EventMessage> {new EventMessage { Body = new string('a', bodyLength) } });
                    await Persistence.CommitAsync(attempt, CancellationToken.None).ConfigureAwait(false);

                    var commits = (await Persistence.GetFromAsync(CancellationToken.None)).Single();
                    commits.Events.Single().Body.ToString().Length.Should().Be(bodyLength);
                });
        }
    }

    public class PersistenceEngineConcern : SpecificationBase, IClassFixture<PersistenceEngineFixture>
    {
        private readonly PersistenceEngineFixture _fixture;

        public PersistenceEngineConcern(PersistenceEngineFixture fixtureData)
        {
            _fixture = fixtureData;
            _fixture.Initialize(ConfiguredPageSizeForTesting, CancellationToken.None)
                .GetAwaiter().GetResult();
        }

        protected IPersistStreams Persistence => _fixture.Persistence;

        protected int ConfiguredPageSizeForTesting => 2;
    }

    public partial class PersistenceEngineFixture : IDisposable
    {
#pragma warning disable 649
        private readonly Func<int, IPersistStreams> _createPersistence;
#pragma warning restore 649

        public async Task Initialize(int pageSize, CancellationToken cancellationToken)
        {
            if (Persistence != null && !Persistence.IsDisposed)
            {
                await Persistence.DropAsync(cancellationToken).ConfigureAwait(false);
                Persistence.Dispose();
                Persistence = null;
            }

            cancellationToken.ThrowIfCancellationRequested();

            Persistence = new PerformanceCounterPersistenceEngine(_createPersistence(pageSize), "tests");
            await Persistence.InitializeAsync(cancellationToken);
        }

        public IPersistStreams Persistence { get; private set; }

        public async void Dispose()
        {
            if (Persistence != null && !Persistence.IsDisposed)
            {
                await Persistence.DropAsync(CancellationToken.None);
                Persistence.Dispose();
                Persistence = null;
            }
        }
    }
    // ReSharper restore InconsistentNaming
}
