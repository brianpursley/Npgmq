using Microsoft.Extensions.DependencyInjection;

namespace Npgmq;

internal interface INpgmqHostingQueueHandlerOptions
{
    Task PollBatchAsync(
        NpgmqClient client,
        IServiceScopeFactory serviceScopeFactory,
        CancellationToken cancellationToken);
}
