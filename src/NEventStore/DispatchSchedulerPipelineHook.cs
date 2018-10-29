namespace NEventStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using NEventStore.Dispatcher;

    public sealed class DispatchSchedulerPipelineHook : PipelineHookBase
    {
        private readonly IScheduleDispatches _scheduler;

        public DispatchSchedulerPipelineHook(IScheduleDispatches scheduler = null)
        {
            _scheduler = scheduler ?? new NullDispatcher(); // serves as a scheduler also
        }

        public override void Dispose()
        {
            _scheduler.Dispose();
        }

        public override Task PostCommitAsync(ICommit committed, CancellationToken cancellationToken)
        {
            if (committed != null)
            {
                _scheduler.ScheduleDispatch(committed, cancellationToken);
            }

            return Task.CompletedTask;
        }
    }
}