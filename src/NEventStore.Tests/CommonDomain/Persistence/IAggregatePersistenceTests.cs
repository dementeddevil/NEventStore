namespace CommonDomain
{
	using System;
	using System.Threading.Tasks;
	using CommonDomain.Core;
	using CommonDomain.Persistence;
	using CommonDomain.Persistence.EventStore;
	using FluentAssertions;
	using NEventStore;
	using NEventStore.Persistence.AcceptanceTests;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using Xunit;

	public class using_a_configured_repository : SpecificationBase
	{
        protected IRepository Repository;

        protected IStoreEvents StoreEvents;

        protected override Task Context()
		{
            StoreEvents = Wireup.Init().UsingInMemoryPersistence().Build();
            Repository = new EventStoreRepository(StoreEvents, new AggregateFactory(), new ConflictDetector());
			return base.Context();
		}
	}

	public class when_an_aggregate_is_persisted : using_a_configured_repository
	{
        private Guid _id;
		private TestAggregate _testAggregate;

        protected override async Task Context()
		{
            await base.Context();
			_id = Guid.NewGuid();
			_testAggregate = new TestAggregate(_id, "Test");
		}

        protected override Task Because()
		{
            return Repository.Save(_testAggregate, Guid.NewGuid(), null);
		}

		[Fact]
        public async Task should_be_returned_when_loaded_by_id()
		{
            (await Repository.GetById<TestAggregate>(_id)).Name.Should().Be(_testAggregate.Name);
		}
	}

	public class when_a_persisted_aggregate_is_updated : using_a_configured_repository
	{
        private const string NewName = "UpdatedName";
		private Guid _id;

        protected override Task Context()
		{
			base.Context();
			_id = Guid.NewGuid();
            return Repository.Save(new TestAggregate(_id, "Test"), Guid.NewGuid(), null);
		}

        protected override async Task Because()
		{
            var aggregate = await Repository.GetById<TestAggregate>(_id);
			aggregate.ChangeName(NewName);
            await Repository.Save(aggregate, Guid.NewGuid(), null);
		}

		[Fact]
        public async Task should_have_updated_name()
		{
            var item = await Repository.GetById<TestAggregate>(_id);
			item.Name.Should().Be(NewName);
		}

		[Fact]
        public async Task should_have_updated_version()
		{
			var item = await Repository.GetById<TestAggregate>(_id);
			item.Version.Should().Be(2);
		}
	}

	public class when_a_loading_a_specific_aggregate_version : using_a_configured_repository
	{
		private const string VersionOneName = "Test";
		private const string NewName = "UpdatedName";
        private Guid _id;

        protected override async Task Context()
		{
            await base.Context();
			_id = Guid.NewGuid();
            await Repository.Save(new TestAggregate(_id, VersionOneName), Guid.NewGuid(), null);
		}

        protected override async Task Because()
		{
            var aggregate = await Repository.GetById<TestAggregate>(_id);
			aggregate.ChangeName(NewName);
            await Repository.Save(aggregate, Guid.NewGuid(), null);
            Repository.Dispose();
		}

		[Fact]
        public async Task should_be_able_to_load_initial_version()
		{
            var item = await Repository.GetById<TestAggregate>(_id, 1);
			item.Name.Should().Be(VersionOneName);
		}
	}

	public class when_an_aggregate_is_persisted_to_specific_bucket : using_a_configured_repository
	{
        private string _bucket;
        private Guid _id;
		private TestAggregate _testAggregate;

        protected override Task Context()
		{
			_id = Guid.NewGuid();
			_bucket = "TenantB";
			_testAggregate = new TestAggregate(_id, "Test");
            return base.Context();
		}

        protected override Task Because()
		{
            return Repository.Save(_bucket, _testAggregate, Guid.NewGuid(), null);
		}

		[Fact]
        public async Task should_be_returned_when_loaded_by_id()
		{
            var item = await Repository.GetById<TestAggregate>(_bucket, _id);
			item.Name.Should().Be(_testAggregate.Name);
		}
	}

    public class when_an_aggregate_is_persisted_concurrently_by_two_clients : SpecificationBase
    {
        private Guid _aggregateId;
        protected IRepository _repository1;
        protected IRepository _repository2;

        protected IStoreEvents _storeEvents;
        private Exception _thrown;

        protected override async Task Context()
        {
            await base.Context();

            _storeEvents = Wireup.Init().UsingInMemoryPersistence().Build();
            _repository1 = new EventStoreRepository(_storeEvents, new AggregateFactory(), new ConflictDetector());
            _repository2 = new EventStoreRepository(_storeEvents, new AggregateFactory(), new ConflictDetector());

            _aggregateId = Guid.NewGuid();
            var aggregate = new TestAggregate(_aggregateId, "my name is..");
            await _repository1.Save(aggregate, Guid.NewGuid());
        }

        protected override async Task Because()
        {
            var agg1 = await _repository1.GetById<TestAggregate>(_aggregateId);
            var agg2 = await _repository2.GetById<TestAggregate>(_aggregateId);
            agg1.ChangeName("one");
            agg2.ChangeName("two");

            await _repository1.Save(agg1, Guid.NewGuid());

            _thrown = await Catch.Exception(() => _repository2.Save(agg2, Guid.NewGuid()));
        }

        [Fact]
        public void should_throw_a_ConflictingCommandException()
        {
            _thrown.Should().BeOfType<ConflictingCommandException>();
        }        
    }
}