namespace NEventStore.Dispatcher
{
    using System;
    using System.Threading;
    using NEventStore.Logging;

    public sealed class NullDispatcher : IScheduleDispatches, IDispatchCommits
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (NullDispatcher));

        public void Dispatch(ICommit commit)
        {
            Logger.Info(Resources.DispatchingToDevNull);
        }

        public void Dispose()
        {
            Logger.Debug(Resources.ShuttingDownDispatcher);
            GC.SuppressFinalize(this);
        }

        public void ScheduleDispatch(ICommit commit, CancellationToken cancellationToken)
        {
            Logger.Info(Resources.SchedulingDispatch, commit.CommitId);
            Dispatch(commit);
        }

        public void Start(CancellationToken cancellationToken)
        {
            //No-op
        }
    }
}