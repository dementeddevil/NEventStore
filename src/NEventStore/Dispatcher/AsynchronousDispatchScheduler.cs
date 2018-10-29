namespace NEventStore.Dispatcher
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using NEventStore.Logging;
    using NEventStore.Persistence;

    public class AsynchronousDispatchScheduler : SynchronousDispatchScheduler
    {
        private const int BoundedCapacity = 1024;
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (AsynchronousDispatchScheduler));
        private readonly BlockingCollection<ICommit> _queue;
        private Task _worker;

        public AsynchronousDispatchScheduler(IDispatchCommits dispatcher, IPersistStreams persistence)
            : base(dispatcher, persistence)
        {
            _queue = new BlockingCollection<ICommit>(new ConcurrentQueue<ICommit>(), BoundedCapacity);
        }

        public override void Start(CancellationToken cancellationToken)
        {
            _worker = Task.Factory.StartNew(Working, cancellationToken);
            base.Start(cancellationToken);
        }

        public override void ScheduleDispatch(ICommit commit, CancellationToken cancellationToken)
        {
            if (!Started)
            {
                throw new InvalidOperationException(Messages.SchedulerNotStarted);
            }
            Logger.Info(Resources.SchedulingDelivery, commit.CommitId);
            _queue.Add(commit, cancellationToken);
        }

        private void Working()
        {
            foreach (var commit in _queue.GetConsumingEnumerable())
            {
                base.ScheduleDispatch(commit, CancellationToken.None);
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            _queue.CompleteAdding();
            _worker?.Wait(TimeSpan.FromSeconds(30));
        }
    }
}