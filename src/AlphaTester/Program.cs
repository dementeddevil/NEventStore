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
		static void Main( string[] args )
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

			Console.WriteLine(string.Format("using [{0}] storage", repoType));
			var startTime = DateTime.UtcNow;

			var options = new ParallelOptions();
			options.MaxDegreeOfParallelism = 10;
			Parallel.For(0, num, options, (i) =>
			{
				Stopwatch sw = new Stopwatch();
				sw.Start();
				Guid start = Guid.NewGuid();

				TestRepository repo = new TestRepository(repoType);
				var aggy = SimpleAggregate.CreateNew(DateTime.Now, start, 42);
				repo.Save(aggy, Guid.NewGuid(), null);

				Random random = new Random();
				Parallel.For(0, 5, options, (j) =>
				{
					var thisIter = random.Next();

					try
					{
						//Console.WriteLine("starting {0}", thisIter);
						repo = new TestRepository(repoType);
						aggy = repo.GetSimpleAggregateById(start, 0);
						aggy.ChangeFoo(52);
						//Thread.Sleep(random.Next(100, 400));	// this is to increase race condition likelyhood
						repo.Save(aggy, Guid.NewGuid(), null);
						//Console.WriteLine("success {0}", thisIter);
					}
					catch (Exception ex)
					{ Console.WriteLine("error iteration {0}, {1} - {2}", i, thisIter, ex.ToString()); }
				});

				Console.WriteLine(string.Format("Iteration [{0}] took me [{1}] ms", i, sw.ElapsedMilliseconds));
			});

			var endTime = DateTime.UtcNow.AddSeconds( -10 );
			var repo2 = new TestRepository(eRepositoryType.AzureBlob);
			List<ICommit> commits = repo2.GetSimpleAggregateFromTo( startTime, endTime );
		}
	}
}
