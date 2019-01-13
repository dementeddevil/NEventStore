namespace NEventStore
{
    using FluentAssertions;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using System;
    using Xunit;
	using System.Threading.Tasks;

    public class DefaultSerializationWireupTests
    {
        public class when_building_an_event_store_without_an_explicit_serializer : SpecificationBase
        {
            private Wireup _wireup;
            private Exception _exception;
            private IStoreEvents _eventStore;

            protected override Task Context()
            {
                _wireup = Wireup.Init()
                    .UsingSqlPersistence("fakeConnectionString")
                        .WithDialect(new Persistence.Sql.SqlDialects.MsSqlDialect());
				return Task.FromResult(true);
            }

            protected override Task Because()
            {
                _exception = Catch.Exception(() => { _eventStore = _wireup.Build(); });
				return Task.FromResult(true);
            }

            protected override void Cleanup()
            {
                _eventStore.Dispose();
            }

            [Fact]
            public void should_not_throw_an_argument_null_exception()
            {
                if (_exception != null)
                {
                    _exception.GetType().Should().NotBe<ArgumentNullException>();
                }
            }
        }
    }
}
