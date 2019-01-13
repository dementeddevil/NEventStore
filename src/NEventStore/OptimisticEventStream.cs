namespace NEventStore
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics.CodeAnalysis;
	using System.Linq;
	using System.Threading.Tasks;
	using NEventStore.Logging;

	[SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix",
		Justification = "This behaves like a stream--not a .NET 'Stream' object, but a stream nonetheless.")]
	public sealed class OptimisticEventStream : IEventStream
	{
		private static readonly ILog Logger = LogFactory.BuildLogger(typeof(OptimisticEventStream));
		private readonly ICollection<EventMessage> _committed = new LinkedList<EventMessage>();
		private readonly IDictionary<string, object> _committedHeaders = new Dictionary<string, object>();
		private readonly ICollection<EventMessage> _events = new LinkedList<EventMessage>();
		private readonly ICollection<Guid> _identifiers = new HashSet<Guid>();
		private readonly ICommitEvents _persistence;
		private readonly IDictionary<string, object> _uncommittedHeaders = new Dictionary<string, object>();
		private bool _disposed;

		public OptimisticEventStream(string bucketId, string streamId, ICommitEvents persistence)
		{
			BucketId = bucketId;
			StreamId = streamId;
			_persistence = persistence;
		}

		public string BucketId
		{
			get;
			private set;
		}
		public string StreamId
		{
			get;
			private set;
		}
		public int StreamRevision
		{
			get;
			private set;
		}
		public int CommitSequence
		{
			get;
			private set;
		}

		public ICollection<EventMessage> CommittedEvents
		{
			get
			{
				return new ImmutableCollection<EventMessage>(_committed);
			}
		}

		public IDictionary<string, object> CommittedHeaders
		{
			get
			{
				return _committedHeaders;
			}
		}

		public ICollection<EventMessage> UncommittedEvents
		{
			get
			{
				return new ImmutableCollection<EventMessage>(_events);
			}
		}

		public IDictionary<string, object> UncommittedHeaders
		{
			get
			{
				return _uncommittedHeaders;
			}
		}

		public async Task Initialize(int minRevision, int maxRevision)
		{
			IEnumerable<ICommit> commits = await _persistence.GetFrom(BucketId, StreamId, minRevision, maxRevision);
			PopulateStream(minRevision, maxRevision, commits);

			if (minRevision > 0 && _committed.Count == 0)
			{
				throw new StreamNotFoundException();
			}
		}

		public async Task Initialize(ISnapshot snapshot, int maxRevision)
		{
			if (snapshot.BucketId != BucketId || snapshot.StreamId != StreamId)
			{
				throw new ArgumentException("Invalid snapshot.");
			}

			IEnumerable<ICommit> commits = await _persistence.GetFrom(snapshot.BucketId, snapshot.StreamId, snapshot.StreamRevision, maxRevision);
			PopulateStream(snapshot.StreamRevision + 1, maxRevision, commits);
			StreamRevision = snapshot.StreamRevision + _committed.Count;
		}

		public void Add(EventMessage uncommittedEvent)
		{
			if (uncommittedEvent == null || uncommittedEvent.Body == null)
			{
				return;
			}

			Logger.Debug(Resources.AppendingUncommittedToStream, StreamId);
			_events.Add(uncommittedEvent);
		}

		public async Task CommitChanges(Guid commitId)
		{
			Logger.Debug(Resources.AttemptingToCommitChanges, StreamId);

			if (_identifiers.Contains(commitId))
			{
				throw new DuplicateCommitException();
			}

			if (!HasChanges())
			{
				return;
			}

			ConcurrencyException exceptionToThrow = null;
			try
			{
				await PersistChanges(commitId);
			}
			catch (ConcurrencyException exception)
			{
				exceptionToThrow = exception;
				Logger.Info(Resources.UnderlyingStreamHasChanged, StreamId);
			}

			if (exceptionToThrow != null)
			{
				IEnumerable<ICommit> commits = await _persistence.GetFrom(BucketId, StreamId, StreamRevision + 1, int.MaxValue);
				PopulateStream(StreamRevision + 1, int.MaxValue, commits);

				throw exceptionToThrow;
			}
		}

		public void ClearChanges()
		{
			Logger.Debug(Resources.ClearingUncommittedChanges, StreamId);
			_events.Clear();
			_uncommittedHeaders.Clear();
		}

		private void PopulateStream(int minRevision, int maxRevision, IEnumerable<ICommit> commits)
		{
			foreach (var commit in commits ?? Enumerable.Empty<ICommit>())
			{
				Logger.Verbose(Resources.AddingCommitsToStream, commit.CommitId, commit.Events.Count, StreamId);
				_identifiers.Add(commit.CommitId);

				CommitSequence = commit.CommitSequence;
				int currentRevision = commit.StreamRevision - commit.Events.Count + 1;
				if (currentRevision > maxRevision)
				{
					return;
				}

				CopyToCommittedHeaders(commit);
				CopyToEvents(minRevision, maxRevision, currentRevision, commit);
			}
		}

		private void CopyToCommittedHeaders(ICommit commit)
		{
			foreach (var key in commit.Headers.Keys)
			{
				_committedHeaders[key] = commit.Headers[key];
			}
		}

		private void CopyToEvents(int minRevision, int maxRevision, int currentRevision, ICommit commit)
		{
			foreach (var @event in commit.Events)
			{
				if (currentRevision > maxRevision)
				{
					Logger.Debug(Resources.IgnoringBeyondRevision, commit.CommitId, StreamId, maxRevision);
					break;
				}

				if (currentRevision++ < minRevision)
				{
					Logger.Debug(Resources.IgnoringBeforeRevision, commit.CommitId, StreamId, maxRevision);
					continue;
				}

				_committed.Add(@event);
				StreamRevision = currentRevision - 1;
			}
		}

		private bool HasChanges()
		{
			if (_disposed)
			{
				throw new ObjectDisposedException(Resources.AlreadyDisposed);
			}

			if (_events.Count > 0)
			{
				return true;
			}

			Logger.Warn(Resources.NoChangesToCommit, StreamId);
			return false;
		}

		private async Task PersistChanges(Guid commitId)
		{
			CommitAttempt attempt = BuildCommitAttempt(commitId);

			Logger.Debug(Resources.PersistingCommit, commitId, StreamId);
			ICommit commit = await _persistence.Commit(attempt);

			if (commit != null)
			{
				PopulateStream(StreamRevision + 1, attempt.StreamRevision, new[] { commit });
			}

			ClearChanges();
		}

		private CommitAttempt BuildCommitAttempt(Guid commitId)
		{
			Logger.Debug(Resources.BuildingCommitAttempt, commitId, StreamId);
			return new CommitAttempt(
				BucketId,
				StreamId,
				StreamRevision + _events.Count,
				commitId,
				CommitSequence + 1,
				SystemTime.UtcNow,
				_uncommittedHeaders.ToDictionary(x => x.Key, x => x.Value),
				_events.ToList());
		}

		public void Dispose()
		{
			_disposed = true;
		}
	}
}