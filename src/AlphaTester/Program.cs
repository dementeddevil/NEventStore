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
			//Guid start = Guid.Parse( "9b98f232-2369-48ba-99f6-20a4e3397434" );


			Stopwatch sw = new Stopwatch();
			for ( int i = 0; i != 3; ++i )
			{
				Guid start = Guid.NewGuid();

				TestRepository repo = new TestRepository();
				SimpleAggregate aggy = repo.GetSimpleAggregateById( start, 0 );
				if ( aggy == null || aggy.Id == Guid.Empty )
				{
					aggy = SimpleAggregate.CreateNew( DateTime.Now, start, 42 );
					repo.Save( aggy, Guid.NewGuid(), null );
				}
				sw.Restart();

				for ( int j = 0; j<= 20; ++j )
				{
					repo = new TestRepository();
					aggy = repo.GetSimpleAggregateById( start, 0 );
					aggy.ChangeFoo( 52 );
					repo.Save( aggy, Guid.NewGuid(), null );
				}

				//	repo = new TestRepository();
				//aggy = repo.GetSimpleAggregateById( start, 0 );
				//if ( aggy == null || aggy.Id == Guid.Empty )
				//{
				//	aggy = SimpleAggregate.CreateNew( DateTime.Now, start, 42 );
				//	repo.Save( aggy, Guid.NewGuid(), null );
				//}
				//repo = new TestRepository();
				//aggy = repo.GetSimpleAggregateById( start, 0 );

				//aggy.ChangeFoo( 52 );
				//repo.Save( aggy, Guid.NewGuid(), null );

				//repo = new TestRepository();
				//aggy = repo.GetSimpleAggregateById( start, 0 );
				//aggy.ChangeFoo( 62 );
				//repo.Save( aggy, Guid.NewGuid(), null );

				//repo = new TestRepository();
				//aggy = repo.GetSimpleAggregateById( start, 0 );
				//aggy.ChangeFoo( 72 );
				//repo.Save( aggy, Guid.NewGuid(), null );

				//repo = new TestRepository();
				//aggy = repo.GetSimpleAggregateById( start, 0 );
				//aggy.ChangeFoo( 82 );
				//repo.Save( aggy, Guid.NewGuid(), null );

				//repo = new TestRepository();
				//aggy = repo.GetSimpleAggregateById( start, 0 );
				//aggy.ChangeFoo( 92 );
				//repo.Save( aggy, Guid.NewGuid(), null );

				//TestRepository repo2 = new TestRepository();

				Console.WriteLine( string.Format( "It took me [{0}] ms", sw.ElapsedMilliseconds ) );
			}

			//var newAggy = repo2.GetSimpleAggregateById( start, 3 );
			//repo = new TestRepository();
			//aggy = repo.GetSimpleAggregateById( start, 0 );
		}
	}
}
