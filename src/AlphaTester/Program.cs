using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NEventStore;
using NLog;

namespace AlphaTester
{
	class Program
	{
		private static readonly Logger _log = LogManager.GetCurrentClassLogger(typeof(Program));

		static void Main(string[] args)
		{
			var repoType = eRepositoryType.AzureBlob;
			if (args.Length > 0)
			{
				if (args[0].Equals("sql", StringComparison.OrdinalIgnoreCase))
				{ repoType = eRepositoryType.Sql; }
			}

			var num = 10;
			if (args.Length > 1)
			{ num = Convert.ToInt32(args[1]); }

			var startTime = DateTime.UtcNow;
			var options = new ParallelOptions();
			options.MaxDegreeOfParallelism = 10;
			var eventNum = 25;
			Guid start = Guid.NewGuid();
			Parallel.For(0, num, options, (i) =>
			{
				Stopwatch sw = new Stopwatch();
				sw.Start();
				
				var repo = new TestRepository(repoType);

				Stopwatch creationTimer = new Stopwatch();
				creationTimer.Start();
				var aggy = SimpleAggregate.CreateNew(DateTime.Now, start, 42);
				repo.Save(aggy, Guid.NewGuid(), null);
				creationTimer.Stop();
				_log.Trace("Create aggy in [{0}]", creationTimer.Elapsed);

				Random random = new Random();
				for (int j = 0; j != eventNum; ++j)
				//Parallel.For(0, eventNum, options, (j) =>
				{
					try
					{ RetryWhileConcurrent(repoType, start, i, j); }
					catch (Exception ex)
					{ _log.Error("error iteration {0}-{1}, {2}", i, j, ex.ToString()); }

				}//);

				_log.Trace(string.Format("Iteration [{0}] took me [{1}] ms", i, sw.ElapsedMilliseconds));
			});

			// Check that aggy is still valid
			try
			{
				var repo = new TestRepository(repoType);
				var aggy = repo.GetSimpleAggregateById(start, 0);
				var listOfInts = aggy.FooHolder;
				for (int i = 0; i < eventNum; ++i)
				{
					if (!listOfInts.Contains(i))
					{ _log.Error("Aggy missing value {0}.  Event not rehydrated correctly", i); }
				}

				_log.Info("Aggy check done, id: {0}", start);
			}
			catch (Exception ex)
			{ _log.Error("error in aggy valid check, {0}", ex.ToString()); }
		}

		private static void RetryWhileConcurrent(eRepositoryType repoType, Guid aggyId, int rootIndex, int subIndex)
		{
			while (true)
			{
				Random random = new Random();

				try
				{
					Stopwatch sw = new Stopwatch();
					sw.Start();

					_log.Trace("{0}-{1} - Getting Aggregate");
					var repo = new TestRepository(repoType);
					var aggy = repo.GetSimpleAggregateById(aggyId, 0);
					aggy.ChangeFoo(subIndex);

					_log.Trace("{0}-{1} - Saving Aggregate");
					repo.Save(aggy, Guid.NewGuid(), null);

					_log.Trace("{0}-{1} - Finished in {2}", rootIndex, subIndex, sw.Elapsed);
					break;
				}
				catch (ConcurrencyException)
				{
					_log.Trace("Concurrency Detected, will retry shortly");
					Thread.Sleep(random.Next(0, 200));	// this is to increase race condition likelyhood
				}
			}
		}
	}
}
