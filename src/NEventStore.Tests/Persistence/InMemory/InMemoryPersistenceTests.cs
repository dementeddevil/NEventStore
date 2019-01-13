namespace NEventStore.Persistence.InMemory
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using FluentAssertions;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using Xunit;

    public class when_getting_from_to_then_should_not_get_later_commits : SpecificationBase
    {
        private readonly DateTime _endDate = new DateTime(2013, 1, 2);
        private readonly DateTime _startDate = new DateTime(2013, 1, 1);
        private ICommit[] _commits;
        private InMemoryPersistenceEngine _engine;

        protected override Task Context()
        {
            _engine = new InMemoryPersistenceEngine();
            _engine.Initialize();
            var streamId = Guid.NewGuid().ToString();
            _engine.Commit(new CommitAttempt(streamId, 1, Guid.NewGuid(), 1, _startDate, new Dictionary<string, object>(), new List<EventMessage>{ new EventMessage()}));
            _engine.Commit(new CommitAttempt(streamId, 2, Guid.NewGuid(), 2, _endDate, new Dictionary<string, object>(), new List<EventMessage>{ new EventMessage()}));
			return Task.FromResult(true);
        }

        protected override async Task Because()
        {
            _commits = (await _engine.GetFromTo(_startDate, _endDate)).ToArray();
        }

        [Fact]
        public void should_return_two_commits()
        {
            _commits.Length.Should().Be(1);
        }
    }
}