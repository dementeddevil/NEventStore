using System;
using System.Collections.Generic;
using CommonDomain.Core;

namespace AlphaTester
{
	[Serializable]
	public class SimpleAggregate : AggregateBase
	{
		private List<int> _fooHolder;
		private DateTime _createdDate;
		private DateTime _fooChangedDate;
		private int _numberOfEventsApplied;

		public List<int> FooHolder
		{ get { return _fooHolder; } }

		public int NumberOfEvents
		{ get { return _numberOfEventsApplied; } }

		public static SimpleAggregate CreateNew( DateTime createdDate, Guid aggregateId, int foo )
		{ return new SimpleAggregate( aggregateId, createdDate, foo ); }

		public SimpleAggregate( Guid aggregateId, DateTime createdDate, int foo )
		{
			Id = aggregateId;
			_fooHolder = new List<int>();
			_createdDate = createdDate;
			_fooChangedDate = createdDate;

			var theEvent = new NewSimpleCreatedEvent( aggregateId, createdDate, foo );
			RaiseEvent( theEvent );
		}

		public SimpleAggregate()
		{
			_fooHolder = new List<int>();
			_createdDate = DateTime.MinValue;
			_fooChangedDate = DateTime.MinValue;
		}

		public void ChangeFoo( int newFoo )
		{
			var theEvent = new FooChangedEvent( newFoo );
			RaiseEvent( theEvent );
		}

		private void Apply( NewSimpleCreatedEvent theEvent )
		{
			Id = theEvent.Id;
			_createdDate = theEvent.CreatedDate;
			_fooHolder.Add(theEvent.Foo);
			++_numberOfEventsApplied;
		}

		private void Apply( FooChangedEvent theEvent )
		{
			_fooHolder.Add(theEvent.NewFoo);
			_fooChangedDate = DateTime.Now;
			++_numberOfEventsApplied;
		}
	}
}
