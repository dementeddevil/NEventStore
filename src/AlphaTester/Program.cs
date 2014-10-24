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
			var eventNum = 9;
			Guid start = Guid.NewGuid();
			Parallel.For(0, num, options, (i) =>
			{
				Stopwatch sw = new Stopwatch();
				sw.Start();
				

				var repo = new TestRepository(repoType);
				var aggy = SimpleAggregate.CreateNew(DateTime.Now, start, 42);
				repo.Save(aggy, Guid.NewGuid(), null);

				Random random = new Random();
				//for (int j = 0; j != 10; ++j)
				Parallel.For(0, eventNum, options, (j) =>
				{
					var swInner = new Stopwatch();
					swInner.Start();

					try
					{
						//Console.WriteLine("starting {0}-{1}", i, j);
						while (true)
						{
							try
							{
								repo = new TestRepository(repoType);
								aggy = repo.GetSimpleAggregateById(start, 0);
								aggy.ChangeFoo(j);
								Thread.Sleep(random.Next(100, 400));	// this is to increase race condition likelihood
								repo.Save(aggy, Guid.NewGuid(), null);
								break;
							}
							catch (ConcurrencyException cex)
							{ Console.WriteLine("Caught concurrency exception.  Iteration {0}-{1}.", i, j); }
						}
						//Console.WriteLine("starting {0}-{1}", i, j);
					}
					catch (Exception ex)
					{ Console.WriteLine("error iteration {0}-{1}, {2}", i, j, ex.ToString()); }
					finally
					{
						swInner.Stop();
						Console.WriteLine("Iteration [{0}] Took [{1}]", j, swInner.Elapsed);
					}
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
				//aggy.ChangeFoo(52);
				//repo.Save(aggy, Guid.NewGuid(), null);
				Console.WriteLine("Aggy check passed, id: {0}", start);
				//aggy = repo.GetSimpleAggregateById(Guid.Parse("1a5e3fe3-b906-4764-a259-0db2c39c15cd"), 0);
				//aggy.ChangeFoo(23);
				//repo.Save(aggy, Guid.NewGuid(), null);
			}
			catch (Exception ex)
			{ Console.WriteLine("error in aggy valid check, {0}", ex.ToString()); }


			var endTime = DateTime.UtcNow.AddSeconds( -10 );
			var repo2 = new TestRepository(eRepositoryType.AzureBlob);
			List<ICommit> commits = repo2.GetSimpleAggregateFromTo( startTime, endTime );
		}
	}
}
