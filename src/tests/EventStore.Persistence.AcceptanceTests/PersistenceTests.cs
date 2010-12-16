#pragma warning disable 169
// ReSharper disable InconsistentNaming

namespace EventStore.Persistence.AcceptanceTests
{
	using System;
	using System.Configuration;
	using System.Data;
	using System.Data.Common;
	using System.Data.SqlClient;
	using System.Linq;
	using Machine.Specifications;
	using Serialization;
	using SqlPersistence;
	using SqlPersistence.SqlDialects;

	[Subject("Persistence")]
	public class when_a_commit_attempt_is_successfully_committed : using_the_persistence_engine
	{
		static readonly CommitAttempt attempt = new CommitAttempt
		{
			StreamId = streamId,
			CommitId = Guid.NewGuid(),
			StreamName = "Testing",
			Events = { new EventMessage() }
		};

		Because of = () =>
			persistence.Persist(attempt);

		It should_make_the_commit_available_to_be_read_from_the_stream = () =>
			persistence.GetFrom(streamId, 0).First().CommitId.ShouldEqual(attempt.CommitId);

		It should_add_the_commit_to_the_set_of_undispatched_commits;
		It should_increment_the_head_revision_of_the_stream;
	}

	[Subject("Persistence")]
	public class when_a_commit_has_been_marked_as_dispatched
	{
		It should_no_longer_be_found_in_the_set_of_undispatched_commits;
	}

	[Subject("Persistence")]
	public class when_a_snapshot_has_been_added_at_a_specific_commit
	{
		It should_start_reads_from_that_commit;
		It should_no_longer_include_the_corresponding_stream_in_the_set_of_streams_to_snapshot;
	}

	[Subject("Persistence")]
	public class when_reading_from_a_given_revision
	{
		It should_start_from_the_commit_which_contains_the_given_stream_revision;
		It should_read_up_to_the_end_of_the_stream;
		It should_return_the_commits_in_commit_sequence_order_from_oldest_first_to_newest_last;
	}

	[Subject("Persistence")]
	public class when_reading_until_a_given_revision
	{
		It should_start_from_the_commit_of_the_most_recent_snapshot_on_or_before_the_given_revision;
		It should_read_up_to_the_commit_containing_the_given_revision;
		It should_return_the_commits_in_commit_sequence_order_from_oldest_first_to_newest_last;
	}

	[Subject("Persistence")]
	public class when_attempting_to_overwrite_a_committed_sequence
	{
		It should_throw_a_ConcurrencyException;
	}

	[Subject("Persistence")]
	public class when_attempting_to_commit_an_already_committed_attempt
	{
		It should_throw_a_DuplicateCommitException;
	}

	public abstract class using_the_persistence_engine
	{
		protected static Guid streamId = Guid.NewGuid();
		protected static IPersistStreams persistence;

		Establish context = () =>
		{
			persistence = new SqlPersistence(
				new DelegateConnectionFactory(id => OpenConnection()),
				new CommonSqlDialect(),
				new BinarySerializer());
		};

		private static IDbConnection OpenConnection()
		{
			var connectionName = ConfigurationManager.AppSettings["UnderTest"];
			var setting = ConfigurationManager.ConnectionStrings[connectionName];
			var factory = DbProviderFactories.GetFactory(setting.ProviderName);
			var connection = factory.CreateConnection() ?? new SqlConnection();
			connection.ConnectionString = setting.ConnectionString;
			connection.Open();
			return connection;
		}
	}
}

// ReSharper enable InconsistentNaming
#pragma warning restore 169