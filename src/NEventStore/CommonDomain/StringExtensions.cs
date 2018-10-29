namespace CommonDomain
{
	using System;

	internal static class StringExtensions
	{
		public static Guid ToGuid(this string value)
		{
			var guid = Guid.Empty;
			Guid.TryParse(value, out guid);
			return guid;
		}
	}
}