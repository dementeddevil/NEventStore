using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FakeItEasy;
using NEventStore.Persistence;
using NEventStore.Persistence.AcceptanceTests;
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

namespace NEventStore.Client
{
#if MSTEST
	[TestClass]
#endif
	public class CreatingPollingClient2Tests
	{
		[Fact]
		public void When_persist_streams_is_null_then_should_throw()
		{
			Catch.Exception(() => new PollingClient2(null, c => PollingClient2.HandlingResult.MoveToNext)).Should().BeOfType<ArgumentNullException>();
		}

		[Fact]
		public void When_interval_less_than_zero_then_should_throw()
		{
			Catch.Exception(() => new PollingClient2(A.Fake<IPersistStreams>(), null)).Should().BeOfType<ArgumentNullException>();
		}
	}

#if MSTEST
	[TestClass]
#endif
	public class base_handling_committed_events : using_polling_client2
	{
		private List<ICommit> commits = new List<ICommit>();

		protected override Task Context()
		{
			base.Context();
			HandleFunction = c =>
			{
				commits.Add(c);
				return PollingClient2.HandlingResult.MoveToNext;
			};

            return StoreEvents.Advanced.CommitSingleAsync();
        }

		protected override Task Because()
		{
			Sut.StartFrom(0);
            return Task.CompletedTask;
		}

		[Fact]
		public void commits_are_correctly_dispatched()
		{
			WaitForCondition(() => commits.Count >= 1);
			commits.Count.Should().Be(1);
		}
	}

#if MSTEST
	[TestClass]
#endif
	public class base_handling_committed_events_and_new_events : using_polling_client2
	{
		private List<ICommit> commits = new List<ICommit>();

		protected override Task Context()
		{
			base.Context();
			HandleFunction = c =>
			{
				commits.Add(c);
				return PollingClient2.HandlingResult.MoveToNext;
			};

            return StoreEvents.Advanced.CommitSingleAsync();
		}

		protected override async Task Because()
		{
			Sut.StartFrom(0);
			for (int i = 0; i < 15; i++)
			{
				await StoreEvents.Advanced.CommitSingleAsync();
			}
		}

		[Fact]
		public void commits_are_correctly_dispatched()
		{
			WaitForCondition(() => commits.Count >= 16);
			commits.Count.Should().Be(16);
		}
	}

#if MSTEST
	[TestClass]
#endif
	public class verify_stopping_commit_polling_client : using_polling_client2
	{
		private List<ICommit> commits = new List<ICommit>();

		protected override async Task Context()
		{
			await base.Context();
			HandleFunction = c =>
			{
				commits.Add(c);
				return PollingClient2.HandlingResult.Stop;
			};

            await StoreEvents.Advanced.CommitSingleAsync();
			await StoreEvents.Advanced.CommitSingleAsync();
			await StoreEvents.Advanced.CommitSingleAsync();
		}

		protected override Task Because()
		{
			Sut.StartFrom(0);
            return Task.CompletedTask;
		}

		[Fact]
		public void commits_are_correctly_dispatched()
		{
			WaitForCondition(() => commits.Count >= 2, timeoutInSeconds: 1);
			commits.Count.Should().Be(1);
		}
	}

#if MSTEST
	[TestClass]
#endif
	public class verify_retry_commit_polling_client : using_polling_client2
	{
		private List<ICommit> commits = new List<ICommit>();

		protected override Task Context()
		{
			base.Context();
			HandleFunction = c =>
			{
				commits.Add(c);
				if (commits.Count < 3)
					return PollingClient2.HandlingResult.Retry;

				return PollingClient2.HandlingResult.MoveToNext;
			};

			return StoreEvents.Advanced.CommitSingleAsync();
		}

		protected override Task Because()
		{
			Sut.StartFrom(0);
            return Task.CompletedTask;
		}

		[Fact]
		public void commits_are_retried()
		{
			WaitForCondition(() => commits.Count >= 3, timeoutInSeconds: 1);
			commits.Count.Should().Be(3);
			commits.All(c => c.CheckpointToken == 1).Should().BeTrue();
		}
	}

#if MSTEST
	[TestClass]
#endif
	public class verify_retry_then_move_next : using_polling_client2
	{
		private List<ICommit> commits = new List<ICommit>();

		protected override async Task Context()
		{
			base.Context();
			HandleFunction = c =>
			{
				commits.Add(c);
				if (commits.Count < 3 && c.CheckpointToken == 1)
					return PollingClient2.HandlingResult.Retry;

				return PollingClient2.HandlingResult.MoveToNext;
			};

			await StoreEvents.Advanced.CommitSingleAsync();
			await StoreEvents.Advanced.CommitSingleAsync();
		}

		protected override Task Because()
		{
			Sut.StartFrom(0);
            return Task.CompletedTask;
		}

		[Fact]
		public void commits_are_retried_then_move_next()
		{
			WaitForCondition(() => commits.Count >= 4, timeoutInSeconds: 1);
			commits.Count.Should().Be(4);
			commits
				.Select(c => c.CheckpointToken)
				.SequenceEqual(new[] { 1L, 1L, 1, 2 })
				.Should().BeTrue();
		}
	}

#if MSTEST
	[TestClass]
#endif
	public class verify_manual_plling : using_polling_client2
	{
		private List<ICommit> commits = new List<ICommit>();

		protected override async Task Context()
		{
			base.Context();
			HandleFunction = c =>
			{
				commits.Add(c);
				return PollingClient2.HandlingResult.MoveToNext;
			};

			await StoreEvents.Advanced.CommitSingleAsync();
			await StoreEvents.Advanced.CommitSingleAsync();
		}

		protected override Task Because()
		{
			Sut.ConfigurePollingFunction();
			Sut.PollNow();
            return Task.CompletedTask;
		}

		[Fact]
		public void commits_are_retried_then_move_next()
		{
			WaitForCondition(() => commits.Count >= 2, timeoutInSeconds: 3);
			commits.Count.Should().Be(2);
			commits
				.Select(c => c.CheckpointToken)
				.SequenceEqual(new[] { 1L, 2L })
				.Should().BeTrue();
		}
	}

	public abstract class using_polling_client2 : SpecificationBase
	{
		protected const int PollingInterval = 100;
		protected PollingClient2 sut;
		private IStoreEvents _storeEvents;

		protected PollingClient2 Sut
		{
			get { return sut; }
		}

		protected IStoreEvents StoreEvents
		{
			get { return _storeEvents; }
		}

		protected Func<ICommit, PollingClient2.HandlingResult> HandleFunction;

		protected override Task Context()
		{
			HandleFunction = c => PollingClient2.HandlingResult.MoveToNext;
			_storeEvents = Wireup.Init().UsingInMemoryPersistence().Build();
			sut = new PollingClient2(_storeEvents.Advanced, c => HandleFunction(c), PollingInterval);
            return Task.CompletedTask;
		}

		protected override Task Cleanup()
		{
			_storeEvents.Dispose();
			Sut.Dispose();
            return Task.CompletedTask;
		}

        protected void WaitForCondition(Func<Boolean> predicate, Int32 timeoutInSeconds = 4)
		{
			DateTime startTest = DateTime.Now;
			while (!predicate() && DateTime.Now.Subtract(startTest).TotalSeconds < timeoutInSeconds)
			{
				Thread.Sleep(100);
			}
		}
	}
}
