using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NEventStore;

namespace AlphaTester
{
	class Program
	{
		static void Main(string[] args)
		{
			var repoType = eRepositoryType.AzureBlob;
			if (args.Length > 0)
			{
				if (args[0].Equals("sql", StringComparison.OrdinalIgnoreCase))
				{ repoType = eRepositoryType.Sql; }
			}

			var num = 1;
			if (args.Length > 1)
			{ num = Convert.ToInt32(args[1]); }

			var startTime = DateTime.UtcNow;
			Console.WriteLine("Initializing [{0}] Repository", repoType);
			var initSw = new Stopwatch();
			initSw.Start();
			var repoInit = new TestRepository(repoType);
			var dontCare = repoInit.GetSimpleAggregateById(Guid.NewGuid(), Int32.MaxValue);
			initSw.Stop();
			Console.WriteLine("Took {0} to init", initSw.Elapsed.ToString());


			var options = new ParallelOptions();
			options.MaxDegreeOfParallelism = 10;
			var eventNum = 150;
			Guid start = Guid.NewGuid();
			Parallel.For(0, num, options, (i) =>
			{
				Stopwatch sw = new Stopwatch();
				sw.Start();
				

				var repo = new TestRepository(repoType);
				var aggy = SimpleAggregate.CreateNew(DateTime.Now, start, 42);
				repo.Save(aggy, Guid.NewGuid(), null);

				Random random = new Random();
				//for (int j = 0; j != eventNum; ++j)
				Parallel.For(0, eventNum, options, (j) =>
				{
					try
					{ RetryWhileConcurrent(repoType, start, i, j); }
					catch (Exception ex)
					{ Console.WriteLine("error iteration {0}-{1}, {2}", i, j, ex.ToString()); }

				});

				Console.WriteLine(string.Format("Iteration [{0}] took me [{1}] ms", i, sw.ElapsedMilliseconds));
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
					{ Console.WriteLine("Aggy missing value {0}.  Event not rehydrated correctly", i); }
				}

				Console.WriteLine("Aggy check done, id: {0}", start);
			}
			catch (Exception ex)
			{ Console.WriteLine("error in aggy valid check, {0}", ex.ToString()); }
		}

		private static void RetryWhileConcurrent(eRepositoryType repoType, Guid aggyId, int rootIndex, int subIndex)
		{
			while (true)
			{
				Random random = new Random();

				try
				{
					Console.WriteLine("starting {0}-{1}", rootIndex, subIndex);
					var repo = new TestRepository(repoType);

					Stopwatch sw = new Stopwatch();
					sw.Start();
					var aggy = repo.GetSimpleAggregateById(aggyId, 0);
					sw.Stop();
					Console.WriteLine("{0}-{1} - Performed get in [{2}]", rootIndex, subIndex, sw.Elapsed);

					sw.Reset();
					sw.Start();
					aggy.ChangeFoo(subIndex);
					repo.Save(aggy, Guid.NewGuid(), null);
					sw.Stop();
					Console.WriteLine("{0}-{1} -Performed save in [{2}]", rootIndex, subIndex, sw.Elapsed);
					//Console.WriteLine("starting {0}-{1}", i, j);
					break;
				}
				catch (ConcurrencyException)
				{
					Console.WriteLine("Concurrency Detected, will retry shortly");
					Thread.Sleep(random.Next(0, 200));	// this is to increase race condition likelyhood
				}
			}
		}
	}
}
