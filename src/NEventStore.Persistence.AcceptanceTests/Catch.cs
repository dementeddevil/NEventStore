namespace NEventStore.Persistence.AcceptanceTests
{
    using System;
using System.Threading.Tasks;

    public static class Catch
    {
        public static Exception Exception(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                return ex;
            }

            return null;
        }

		public static async Task<Exception> Exception(Func<Task> action)
		{
			try
			{
				await action();
			}
			catch (Exception ex)
			{
				return ex;
			}

			return null;
		}
	}
}