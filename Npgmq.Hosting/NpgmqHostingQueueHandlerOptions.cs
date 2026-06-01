using Microsoft.Extensions.DependencyInjection;

namespace Npgmq;

internal sealed class NpgmqHostingQueueHandlerOptions<TMessage, THandler>(NpgmqHostingQueueOptions queue) : INpgmqHostingQueueHandlerOptions
    where TMessage : class
    where THandler : class, IConsumer<TMessage>
{
    public async Task PollBatchAsync(
        NpgmqClient client,
        IServiceScopeFactory serviceScopeFactory,
        CancellationToken cancellationToken)
    {
        var messages = await client.PollBatchAsync<TMessage>(
            queue.QueueName,
            queue.VisibilityTimeout,
            queue.BatchSize,
            queue.PollTimeoutSeconds,
            queue.PollIntervalMilliseconds,
            cancellationToken);

        foreach (var message in messages)
        {
            await ProcessMessageAsync(queue, client, serviceScopeFactory, message, cancellationToken);
        }
    }

    private static async Task ProcessMessageAsync(
        NpgmqHostingQueueOptions queue,
        NpgmqClient client,
        IServiceScopeFactory serviceScopeFactory,
        NpgmqMessage<TMessage> message,
        CancellationToken cancellationToken)
    {
        await using var scope = serviceScopeFactory.CreateAsyncScope();

        var handler = ActivatorUtilities.GetServiceOrCreateInstance<THandler>(scope.ServiceProvider);

        await handler.ConsumeAsync(message, cancellationToken);

        await client.ArchiveAsync(queue.QueueName, message.MsgId, cancellationToken);
    }
}