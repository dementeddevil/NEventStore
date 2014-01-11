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

            Console.Write(string.Format("using [{0}] storage", repoType));            

			Stopwatch sw = new Stopwatch();
			for ( int i = 0; i != 3; ++i )
			{
				Guid start = Guid.NewGuid();

				TestRepository repo = new TestRepository(repoType);
				SimpleAggregate aggy = repo.GetSimpleAggregateById( start, 0 );
				if ( aggy == null || aggy.Id == Guid.Empty )
				{
					aggy = SimpleAggregate.CreateNew( DateTime.Now, start, 42 );
					repo.Save( aggy, Guid.NewGuid(), null );
				}
				sw.Restart();

				for ( int j = 0; j<= 10; ++j )
				{
                    repo = new TestRepository(repoType);
					aggy = repo.GetSimpleAggregateById( start, 0 );
					aggy.ChangeFoo( 52 );
					repo.Save( aggy, Guid.NewGuid(), null );
				}

				Console.WriteLine( string.Format( "It took me [{0}] ms", sw.ElapsedMilliseconds ) );
			}
		}
	}
}
