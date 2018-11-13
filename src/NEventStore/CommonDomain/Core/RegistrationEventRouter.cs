using System;
using System.Collections.Generic;

namespace NEventStore.CommonDomain.Core
{
    public class RegistrationEventRouter : IRouteEvents
	{
		private readonly IDictionary<Type, Action<object>> _handlers = new Dictionary<Type, Action<object>>();
		private IAggregate _registered;

		public virtual void Register<T>(Action<T> handler)
		{
			_handlers[typeof(T)] = @event => handler((T)@event);
		}

		public virtual void Register(IAggregate aggregate)
		{
		    _registered = aggregate ?? throw new ArgumentNullException(nameof(aggregate));
		}

		public virtual void Dispatch(object eventMessage)
		{
		    if (!_handlers.TryGetValue(eventMessage.GetType(), out var handler))
			{
				_registered.ThrowHandlerNotFound(eventMessage);
			}

			handler(eventMessage);
		}
	}
}