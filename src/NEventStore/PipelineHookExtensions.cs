namespace NEventStore
{
    using System.Threading;
    using System.Threading.Tasks;

    public static class PipelineHookExtensions
    {
        /// <summary>
        /// Invoked when all buckets have been purged.
        /// </summary>
        /// <param name="pipelineHook">The pipeline hook.</param>
        /// <param name="cancellationToken">The cancallation token.</param>
        public static Task OnPurgeAsync(this IPipelineHook pipelineHook, CancellationToken cancellationToken)
        {
            return pipelineHook.OnPurgeAsync(null, cancellationToken);
        }
    }
}