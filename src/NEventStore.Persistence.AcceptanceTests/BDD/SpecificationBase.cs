namespace NEventStore.Persistence.AcceptanceTests.BDD
{
	using System.Threading.Tasks;
	using Xunit;

	[RunWith(typeof(SpecificationBaseRunner))]
	public abstract class SpecificationBase
	{
		protected virtual Task Because()
		{
			return Task.FromResult(true);
		}

		protected virtual void Cleanup()
		{
		}

		protected virtual Task Context()
		{
			return Task.FromResult(true);
		}

		public void OnFinish()
		{
			Cleanup();
		}

		public Task OnStart()
		{
			Context();
			return Because();
		}
	}
}