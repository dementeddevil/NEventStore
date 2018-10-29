namespace NEventStore.Dispatcher
{
    using System.Threading;

    public class NoopDispatcherScheduler : IScheduleDispatches
    {
        public void Dispose()
        {
            // Noop
        }

        public void ScheduleDispatch(ICommit commit, CancellationToken cancellationToken)
        {
            // Noop
        }

        public void Start(CancellationToken cancellationToken)
        {
            // Noop
        }
    }
}