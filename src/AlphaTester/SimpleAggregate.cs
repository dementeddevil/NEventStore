using CommonDomain.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlphaTester
{
	[Serializable]
	public class SimpleAggregate : AggregateBase
	{
		private int _fooHolder;
		private DateTime _createdDate;
		private DateTime _fooChangedDate;

		public static SimpleAggregate CreateNew( DateTime createdDate, Guid aggregateId, int foo )
		{ return new SimpleAggregate( aggregateId, createdDate, foo ); }

		public SimpleAggregate( Guid aggregateId, DateTime createdDate, int foo )
		{
			Id = aggregateId;
			_fooHolder = foo;
			_createdDate = createdDate;
			_fooChangedDate = createdDate;

			var theEvent = new NewSimpleCreatedEvent( aggregateId, createdDate, foo );
			RaiseEvent( theEvent );
		}

		public SimpleAggregate()
		{
			_fooHolder = -1;
			_createdDate = DateTime.MinValue;
			_fooChangedDate = DateTime.MinValue;
		}

		public void ChangeFoo( int newFoo )
		{
			_fooHolder = newFoo;
			var theEvent = new FooChangedEvent( newFoo );
			RaiseEvent( theEvent );
		}

		private void Apply( NewSimpleCreatedEvent theEvent )
		{
			Id = theEvent.Id;
			_createdDate = theEvent.CreatedDate;
			_fooHolder = theEvent.Foo;
		}

		private void Apply( FooChangedEvent theEvent )
		{
			_fooHolder = theEvent.NewFoo;
			_fooChangedDate = DateTime.Now;
		}
	}
}
