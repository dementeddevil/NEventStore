using System;

namespace NEventStore.CommonDomain
{
    internal static class StringExtensions
	{
		public static Guid ToGuid(this string value)
		{
		    Guid.TryParse(value, out var guid);
			return guid;
		}
	}
}