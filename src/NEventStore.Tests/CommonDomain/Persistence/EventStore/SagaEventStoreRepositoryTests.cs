namespace NEventStore.CommonDomain.Persistence.EventStore
{
	using System;
	using System.Threading.Tasks;
	using global::CommonDomain;
	using global::CommonDomain.Persistence;
	using global::CommonDomain.Persistence.EventStore;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using Xunit;
	using FluentAssertions;

	public class using_a_sagaeventstorerepository : SpecificationBase
	{
		protected ISagaRepository _repository;

		protected IStoreEvents _storeEvents;

		protected override Task Context()
		{
			this._storeEvents = Wireup.Init().UsingInMemoryPersistence().Build();
			this._repository = new SagaEventStoreRepository(this._storeEvents, new SagaFactory());
			return Task.FromResult(true);
		}
	}

	public class when_an_aggregate_is_loaded : using_a_sagaeventstorerepository
	{
		private TestSaga _testSaga;

		private string _id;

		protected override Task Context()
		{
			base.Context();
			_id = "something";
			_testSaga = new TestSaga(_id);
			return Task.FromResult(true);
		}

		protected override Task Because()
		{
			return _repository.Save(_testSaga, Guid.NewGuid(), null);
		}

		[Fact]
		public async Task should_be_returned_when_loaded_by_id()
		{
			var result = await _repository.GetById<TestSaga>(_id);
			result.Id.Should().Be(_testSaga.Id);
		}
	}
}
