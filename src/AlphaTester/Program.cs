using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            var num = 10;
            if (args.Length > 1)
            { num = Convert.ToInt32(args[1]); }

            Console.Write(string.Format("using [{0}] storage", repoType));

            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = 2;
            Parallel.For(0, num, options, (i) =>
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                Guid start = Guid.NewGuid();

                TestRepository repo = new TestRepository(repoType);
                var aggy = SimpleAggregate.CreateNew(DateTime.Now, start, 42);
                repo.Save(aggy, Guid.NewGuid(), null);

                var sw2 = new Stopwatch();
                for (int j = 0; j <= 20; ++j)
                {
                    sw2.Restart();
                    repo = new TestRepository(repoType);
                    aggy = repo.GetSimpleAggregateById(start, 0);
                    aggy.ChangeFoo(52);
                    repo.Save(aggy, Guid.NewGuid(), null);
                    //Console.WriteLine(string.Format("\t\tIt took me [{0}] ms", sw2.ElapsedMilliseconds));
                }

                Console.WriteLine(string.Format("Iteration [{0}] took me [{1}] ms", i, sw.ElapsedMilliseconds));
            });
		}
	}
}
