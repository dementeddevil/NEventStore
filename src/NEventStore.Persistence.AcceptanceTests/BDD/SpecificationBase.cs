using System.Threading.Tasks;

namespace NEventStore.Persistence.AcceptanceTests.BDD
{
    using System;

    public abstract class SpecificationBase
    {
        public async Task Execute(Func<Task> actionMethod)
        {
            await Context().ConfigureAwait(false);
            await Because().ConfigureAwait(false);

            await actionMethod().ConfigureAwait(false);

            await Cleanup().ConfigureAwait(false);
        }

        protected virtual Task Context()
        {
            return Task.CompletedTask;
        }

        protected virtual Task Because()
        {
            return Task.CompletedTask;
        }

        protected virtual Task Cleanup()
        {
            return Task.CompletedTask;
        }
    }
}