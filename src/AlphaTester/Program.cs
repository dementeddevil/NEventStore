using NEventStore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

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
			Parallel.For(0, num, options, (i) =>
			{
				Stopwatch sw = new Stopwatch();
				sw.Start();
				Guid start = Guid.NewGuid();

				var repo = new TestRepository(repoType);
				var aggy = SimpleAggregate.CreateNew(DateTime.Now, start, 42);
				repo.Save(aggy, Guid.NewGuid(), null);

				for (int j = 0; j != 10; ++j)
				//Parallel.For(0, 100, options, (j) =>
				{
					var swInner = new Stopwatch();
					swInner.Start();

					try
					{ RetryWhilConcurrent(repoType, start, i, j); }
					catch (Exception ex)
					{ Console.WriteLine("error iteration {0}-{1}, {2}", i, j, ex.ToString()); }
					finally
					{
						swInner.Stop();
						Console.WriteLine("Iteration [{0}] Took [{1}]", j, swInner.Elapsed);
					}
				}//);

				Console.WriteLine(string.Format("Iteration [{0}] took me [{1}] ms", i, sw.ElapsedMilliseconds));
			});

			var endTime = DateTime.UtcNow.AddSeconds( -10 );
			var repo2 = new TestRepository(eRepositoryType.AzureBlob);
			List<ICommit> commits = repo2.GetSimpleAggregateFromTo( startTime, endTime );
		}

		private static void RetryWhilConcurrent(eRepositoryType repoType, Guid aggyId, int rootIndex, int subIndex)
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
					Console.WriteLine("Performed get in [{0}]", sw.Elapsed);

					sw.Reset();
					sw.Start();
					aggy.ChangeFoo(52);
					repo.Save(aggy, Guid.NewGuid(), null);
					sw.Stop();
					Console.WriteLine("Performed save in [{0}]", sw.Elapsed);
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
