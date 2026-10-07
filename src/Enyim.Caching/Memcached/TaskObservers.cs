using System.Threading;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached;

internal static class TaskObservers
{
    public static void Observe(this Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception; // touch to observe if it already faulted
            return;
        }

        task.ContinueWith(
            static t => { _ = t.Exception; },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}