namespace CommonDomain
{
	using System;
	using System.Threading.Tasks;
	using FluentAssertions;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using Xunit;

    public class when_an_aggregate_is_created : SpecificationBase
    {
        private TestAggregate _testAggregate;

        protected override Task Because()
        {
            _testAggregate = new TestAggregate(Guid.NewGuid(), "Test");
			return base.Because();
        }

        [Fact]
        public void should_have_name()
        {
            _testAggregate.Name.Should().Be("Test");
        }

        [Fact]
        public void aggregate_version_should_be_one()
        {
            _testAggregate.Version.Should().Be(1);
        }
    }

    public class when_updating_an_aggregate : SpecificationBase
    {
        private TestAggregate _testAggregate;

        protected override Task Context()
        {
            _testAggregate = new TestAggregate(Guid.NewGuid(), "Test");
			return base.Context();
        }

        protected override Task Because()
        {
            _testAggregate.ChangeName("UpdatedTest");
			return base.Because();
        }

        [Fact]
        public void name_change_should_be_applied()
        {
            _testAggregate.Name.Should().Be("UpdatedTest");
        }

        [Fact]
        public void applying_events_automatically_increments_version()
        {
            _testAggregate.Version.Should().Be(2);
        }
    }
}