namespace NEventStore.Client
{
	using System;
	using System.Collections.Generic;
	using System.Reactive.Linq;
	using System.Reactive.Threading.Tasks;
	using System.Threading.Tasks;
	using FakeItEasy;
	using FluentAssertions;
	using NEventStore.Persistence;
	using NEventStore.Persistence.AcceptanceTests;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using Xunit;

    public class CreatingPollingClientTests
    {
        [Fact]
        public void When_persist_streams_is_null_then_should_throw()
        {
            Catch.Exception(() => new PollingClient(null)).Should().BeOfType<ArgumentNullException>();
        }

        [Fact]
        public void When_interval_less_than_zero_then_should_throw()
        {
            Catch.Exception(() => new PollingClient(A.Fake<IPersistStreams>(),-1)).Should().BeOfType<ArgumentException>();
        }

        [Fact]
        public void When_interval_is_zero_then_should_throw()
        {
            Catch.Exception(() => new PollingClient(A.Fake<IPersistStreams>(), 0)).Should().BeOfType<ArgumentException>();
        }
    }

    public abstract class using_polling_client : SpecificationBase
    {
        protected const int PollingInterval = 100;
        private PollingClient _pollingClient;
        private IStoreEvents _storeEvents;

        protected PollingClient PollingClient
        {
            get { return _pollingClient; }
        }

        protected IStoreEvents StoreEvents
        {
            get { return _storeEvents; }
        }

        protected override Task Context()
        {
            _storeEvents = Wireup.Init().UsingInMemoryPersistence().Build();
            _pollingClient = new PollingClient(_storeEvents.Advanced, PollingInterval);
			return Task.FromResult(true);
        }

        protected override void Cleanup()
        {
            _storeEvents.Dispose();
        }
    }

    public class when_commit_is_comitted_before_subscribing : using_polling_client
    {
        private IObserveCommits _observeCommits;
        private Task<ICommit> _commitObserved;

        protected override Task Context()
        {
            base.Context();
            StoreEvents.Advanced.CommitSingle();
            _observeCommits = PollingClient.ObserveFrom();
            _commitObserved = _observeCommits.FirstAsync().ToTask();
			return Task.FromResult(true);
        }

        protected override Task Because()
        {
            _observeCommits.Start();
			return Task.FromResult(true);
        }

        protected override void Cleanup()
        {
            _observeCommits.Dispose();
        }

        [Fact]
        public void should_observe_commit()
        {
            _commitObserved.Wait(PollingInterval * 2).Should().BeTrue();
        }
    }

    public class when_commit_is_comitted_before_and_after_subscribing : using_polling_client
    {
        private IObserveCommits _observeCommits;
        private Task<ICommit> _twoCommitsObserved;

        protected override Task Context()
        {
            base.Context();
            StoreEvents.Advanced.CommitSingle();
            _observeCommits = PollingClient.ObserveFrom();
            _twoCommitsObserved = _observeCommits.Take(2).ToTask();
			return Task.FromResult(true);
        }

        protected override Task Because()
        {
            _observeCommits.Start();
            return StoreEvents.Advanced.CommitSingle();
        }

        protected override void Cleanup()
        {
            _observeCommits.Dispose();
        }

        [Fact]
        public void should_observe_two_commits()
        {
            _twoCommitsObserved.Wait(PollingInterval * 2).Should().BeTrue();
        }
    }

    public class with_two_observers_and_multiple_commits : using_polling_client
    {
        private IObserveCommits _observeCommits1;
        private IObserveCommits _observeCommits2;
        private Task<ICommit> _observeCommits1Complete;
        private Task<ICommit> _observeCommits2Complete;

        protected override async Task Context()
        {
            await base.Context();
            await StoreEvents.Advanced.CommitSingle();
            _observeCommits1 = PollingClient.ObserveFrom();
            _observeCommits1Complete = _observeCommits1.Take(5).ToTask();

            _observeCommits2 = PollingClient.ObserveFrom();
            _observeCommits2Complete = _observeCommits1.Take(10).ToTask();
        }

        protected override Task Because()
        {
			var tasks = new List<Task>();
            tasks.Add(_observeCommits1.Start());
            tasks.Add(_observeCommits2.Start());
            tasks.Add(Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < 15; i++)
                {
                    await StoreEvents.Advanced.CommitSingle();
                }
            }));
			return Task.WhenAll(tasks.ToArray());
        }

        protected override void Cleanup()
        {
            _observeCommits1.Dispose();
            _observeCommits2.Dispose();
        }

        [Fact]
        public void should_observe_commits_on_first_observer()
        {
            _observeCommits1Complete.Wait(PollingInterval * 10).Should().BeTrue();
        }

        [Fact]
        public void should_observe_commits_on_second_observer()
        {
            _observeCommits2Complete.Wait(PollingInterval * 10).Should().BeTrue();
        }
    }

    public class with_two_subscriptions_on_a_single_observer_and_multiple_commits : using_polling_client
    {
        private IObserveCommits _observeCommits1;
        private Task<ICommit> _observeCommits1Complete;
        private Task<ICommit> _observeCommits2Complete;

        protected override async Task Context()
        {
            await base.Context();
            await StoreEvents.Advanced.CommitSingle();
            _observeCommits1 = PollingClient.ObserveFrom();
            _observeCommits1Complete = _observeCommits1.Take(5).ToTask();
            _observeCommits2Complete = _observeCommits1.Take(10).ToTask();
        }

        protected override Task Because()
        {
			return Task.WhenAll(
				_observeCommits1.Start(),
	            Task.Factory.StartNew(async () =>
				{
					for (int i = 0; i < 15; i++)
					{
						await StoreEvents.Advanced.CommitSingle();
					}
				}));
        }

        protected override void Cleanup()
        {
            _observeCommits1.Dispose();
        }

        [Fact]
        public void should_observe_commits_on_first_observer()
        {
            _observeCommits1Complete.Wait(PollingInterval * 10).Should().BeTrue();
        }

        [Fact]
        public void should_observe_commits_on_second_observer()
        {
            _observeCommits2Complete.Wait(PollingInterval * 10).Should().BeTrue();
        }
    }

    public class when_resuming : using_polling_client
    {
        private IObserveCommits _observeCommits;
        private Task<ICommit> _commitObserved;

        protected override async Task Context()
        {
            await base.Context();
            await StoreEvents.Advanced.CommitSingle();
            _observeCommits = PollingClient.ObserveFrom();
            _commitObserved = _observeCommits.FirstAsync().ToTask();
            await _observeCommits.Start();
            _commitObserved.Wait(PollingInterval * 2);
            _observeCommits.Dispose();

            await StoreEvents.Advanced.CommitSingle();
            string checkpointToken = _commitObserved.Result.CheckpointToken;
            _observeCommits = PollingClient.ObserveFrom(checkpointToken);
        }

        protected override async Task Because()
        {
            await _observeCommits.Start();
            _commitObserved = _observeCommits.FirstAsync().ToTask();
        }

        protected override void Cleanup()
        {
            _observeCommits.Dispose();
        }

        [Fact]
        public void should_observe_commit()
        {
            _commitObserved.Wait(PollingInterval * 2).Should().BeTrue();
        }
    }

    public class when_polling_now : using_polling_client
    {
        private IObserveCommits _observeCommits;
        private Task<ICommit> _commitObserved;

        protected override async Task Context()
        {
            await base.Context();
			await StoreEvents.Advanced.CommitSingle();
            _observeCommits = PollingClient.ObserveFrom();
            _commitObserved = _observeCommits.FirstAsync().ToTask();
        }

        protected override Task Because()
        {
            _observeCommits.PollNow();
			return Task.FromResult(true);
        }

        protected override void Cleanup()
        {
            _observeCommits.Dispose();
        }

        [Fact]
        public void should_observe_commit()
        {
            _commitObserved.Wait(PollingInterval * 2).Should().BeTrue();
        }
    }
}