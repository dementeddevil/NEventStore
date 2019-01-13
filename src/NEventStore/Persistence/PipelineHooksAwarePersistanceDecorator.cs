namespace NEventStore.Persistence
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using NEventStore.Logging;

	public class PipelineHooksAwarePersistanceDecorator : IPersistStreams
	{
		private static readonly ILog Logger = LogFactory.BuildLogger(typeof(PipelineHooksAwarePersistanceDecorator));
		private readonly IPersistStreams _original;
		private readonly IEnumerable<IPipelineHook> _pipelineHooks;

		public PipelineHooksAwarePersistanceDecorator(IPersistStreams original, IEnumerable<IPipelineHook> pipelineHooks)
		{
			if (original == null)
			{
				throw new ArgumentNullException("original");
			}
			if (pipelineHooks == null)
			{
				throw new ArgumentNullException("pipelineHooks");
			}
			_original = original;
			_pipelineHooks = pipelineHooks;
		}

		public void Dispose()
		{
			_original.Dispose();
		}

		public async Task<IEnumerable<ICommit>> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
		{
			return await ExecuteHooks(await _original.GetFrom(bucketId, streamId, minRevision, maxRevision));
		}

		public Task<ICommit> Commit(CommitAttempt attempt)
		{
			return _original.Commit(attempt);
		}

		public Task<ISnapshot> GetSnapshot(string bucketId, string streamId, int maxRevision)
		{
			return _original.GetSnapshot(bucketId, streamId, maxRevision);
		}

		public Task<bool> AddSnapshot(ISnapshot snapshot)
		{
			return _original.AddSnapshot(snapshot);
		}

		public Task<IEnumerable<IStreamHead>> GetStreamsToSnapshot(string bucketId, int maxThreshold)
		{
			return _original.GetStreamsToSnapshot(bucketId, maxThreshold);
		}

		public Task Initialize()
		{
			return _original.Initialize();
		}

		public async Task<IEnumerable<ICommit>> GetFrom(string bucketId, DateTime start)
		{
			return await ExecuteHooks(await _original.GetFrom(bucketId, start));
		}

		public async Task<IEnumerable<ICommit>> GetFrom(string checkpointToken)
		{
			return await ExecuteHooks(await _original.GetFrom(checkpointToken));
		}

		public Task<ICheckpoint> GetCheckpoint(string checkpointToken)
		{
			return _original.GetCheckpoint(checkpointToken);
		}

		public async Task<IEnumerable<ICommit>> GetFromTo(string bucketId, DateTime start, DateTime end)
		{
			return await ExecuteHooks(await _original.GetFromTo(bucketId, start, end));
		}

		public async Task Purge()
		{
			await _original.Purge();
			foreach (var pipelineHook in _pipelineHooks)
			{
				pipelineHook.OnPurge();
			}
		}

		public async Task Purge(string bucketId)
		{
			await _original.Purge(bucketId);
			foreach (var pipelineHook in _pipelineHooks)
			{
				await pipelineHook.OnPurge(bucketId);
			}
		}

		public Task Drop()
		{
			return _original.Drop();
		}

		public async Task DeleteStream(string bucketId, string streamId)
		{
			await _original.DeleteStream(bucketId, streamId);
			foreach (var pipelineHook in _pipelineHooks)
			{
				await pipelineHook.OnDeleteStream(bucketId, streamId);
			}
		}

		public bool IsDisposed
		{
			get
			{
				return _original.IsDisposed;
			}
		}

		private async Task<IEnumerable<ICommit>> ExecuteHooks(IEnumerable<ICommit> commits)
		{
			List<ICommit> results = new List<ICommit>();
			foreach (var commit in commits)
			{
				ICommit filtered = commit;
				foreach (var hook in _pipelineHooks)
				{
					filtered = await hook.Select(filtered);
					if (filtered == null)
					{
						Logger.Info(Resources.PipelineHookSkippedCommit, hook.GetType(), commit.CommitId);
						break;
					}
				}

				if (filtered == null)
				{
					Logger.Info(Resources.PipelineHookFilteredCommit);
				}
				else
				{
					results.Add(filtered);
				}
			}
			return results;
		}
	}
}