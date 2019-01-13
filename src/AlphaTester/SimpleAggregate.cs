using System;
using System.Collections.Generic;
using NEventStore.CommonDomain;
using NEventStore.CommonDomain.Core;
using NEventStore;

namespace AlphaTester
{
    [Serializable]
    public class SimpleAggregate : AggregateBase
    {
        private SimpleAggregateDataContainer _logicContainer = new SimpleAggregateDataContainer();

        public List<int> FooHolder
        { get { return _logicContainer.FooHolder; } }

        public int NumberOfEvents
        { get { return _logicContainer.NumberOfAppliedEvents; } }

        public static SimpleAggregate CreateNew(DateTime createdDate, Guid aggregateId, int foo)
        { return new SimpleAggregate(aggregateId, createdDate, foo); }

        public SimpleAggregate(Guid aggregateId, DateTime createdDate, int foo)
        {
            var theEvent = new NewSimpleCreatedEvent(aggregateId, createdDate, foo);
            RaiseEvent(theEvent);
        }

        public SimpleAggregate()
        { }

        public void ChangeFoo(int newFoo)
        {
            var theEvent = new FooChangedEvent(newFoo);
            RaiseEvent(theEvent);
        }

        private void Apply(NewSimpleCreatedEvent theEvent)
        {
            Id = theEvent.Id;
            _logicContainer = new SimpleAggregateDataContainer();
            _logicContainer.AddFooHolder(theEvent.Foo);
            _logicContainer.IncrementAppliedEventCount();
        }

        private void Apply(FooChangedEvent theEvent)
        {
            _logicContainer.AddFooHolder(theEvent.NewFoo);
            _logicContainer.IncrementAppliedEventCount();
        }

        public IMemento prepareMemento()
        {
            IMemento snapshot = _logicContainer;
            snapshot.Id = this.Id;
            snapshot.Version = this.Version;
            return snapshot;
        }

        internal void ApplySnapshot(IMemento snapshot)
        {
            if (snapshot != null)
            {
                _logicContainer = snapshot as SimpleAggregateDataContainer;
                this.Id = _logicContainer.Id;
                this.Version = _logicContainer.Version;
            }
        }
    }

    /// <summary>
    /// Serializable object allowing for snapshotting
    /// </summary>
    [Serializable]
    public class SimpleAggregateDataContainer : IMemento
    {
        public List<int> FooHolder
        { get; private set; }

        public DateTime CreatedDate
        { get; private set; }

        public DateTime LastModifiedDate
        { get; private set; }

        public int NumberOfAppliedEvents
        { get; private set; }

        public Guid Id
        { get; set; }

        public int Version
        { get; set; }

        public SimpleAggregateDataContainer()
        {
            CreatedDate = DateTime.UtcNow;
            LastModifiedDate = CreatedDate;
            FooHolder = new List<int>();
        }

        public void AddFooHolder(int foo)
        {
            FooHolder.Add(foo);
            LastModifiedDate = DateTime.UtcNow;
        }

        public void IncrementAppliedEventCount()
        {
            ++NumberOfAppliedEvents;
            LastModifiedDate = DateTime.UtcNow;
        }
    }
}
