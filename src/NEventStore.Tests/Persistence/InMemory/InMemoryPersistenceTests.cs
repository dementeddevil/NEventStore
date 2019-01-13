using System.Threading;

namespace NEventStore.Persistence.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;
    using NEventStore.Persistence.AcceptanceTests.BDD;
	using FluentAssertions;
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
	public class when_getting_from_to_then_should_not_get_later_commits : SpecificationBase
    {
        private readonly DateTime _endDate = new DateTime(2013, 1, 2);
        private readonly DateTime _startDate = new DateTime(2013, 1, 1);
        private ICommit[] _commits;
        private InMemoryPersistenceEngine _engine;

        protected override async Task Context()
        {
            _engine = new InMemoryPersistenceEngine();
            await _engine.InitializeAsync(CancellationToken.None);
            var streamId = Guid.NewGuid().ToString();
            await _engine.CommitAsync(new CommitAttempt(streamId, 1, Guid.NewGuid(), 1, _startDate, new Dictionary<string, object>(), new List<EventMessage>{ new EventMessage()}));
            await _engine.CommitAsync(new CommitAttempt(streamId, 2, Guid.NewGuid(), 2, _endDate, new Dictionary<string, object>(), new List<EventMessage>{ new EventMessage()}));
        }

        protected override async Task Because()
        {
            _commits = (await _engine.GetFromToAsync(_startDate, _endDate, CancellationToken.None)).ToArray();
        }

        [Fact]
        public void should_return_two_commits()
        {
            _commits.Length.Should().Be(1);
        }
    }
}