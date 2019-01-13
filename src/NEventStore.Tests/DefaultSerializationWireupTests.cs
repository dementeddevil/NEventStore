using System.Threading.Tasks;

namespace NEventStore
{
	using NEventStore.Persistence.AcceptanceTests;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using System;
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

	public class DefaultSerializationWireupTests
	{
#if MSTEST
		[TestClass]
#endif
		public class when_building_an_event_store_without_an_explicit_serializer : SpecificationBase
		{
			private Wireup _wireup;
			private Exception _exception;
			private IStoreEvents _eventStore;
			protected override Task Context()
			{
				_wireup = Wireup.Init()
					.UsingInMemoryPersistence();
                return Task.CompletedTask;
            }

			protected override Task Because()
			{
				_exception = Catch.Exception(() => { _eventStore = _wireup.Build(); });
                return Task.CompletedTask;
            }

			protected override Task Cleanup()
			{
				_eventStore.Dispose();
                return Task.CompletedTask;
            }

			[Fact]
			public void should_not_throw_an_argument_null_exception()
			{
				// _exception.Should().NotBeOfType<ArgumentNullException>();
				_exception.Should().BeNull();
			}
		}
	}
}
