using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlphaTester
{
	[Serializable]
	public class NewSimpleCreatedEvent
	{
		public Guid Id
		{ get; set; }

		public DateTime CreatedDate
		{ get; set; }

		public int Foo
		{ get; set; }

		public NewSimpleCreatedEvent( Guid aggregateId, DateTime createdDate, int foo )
		{
			Id = aggregateId;
			CreatedDate = createdDate;
			Foo = foo;
		}
	}
}
