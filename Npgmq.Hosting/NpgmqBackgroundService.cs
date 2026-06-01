using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Npgmq;

internal sealed class NpgmqBackgroundService(
    NpgmqClient client,
    IServiceScopeFactory serviceScopeFactory,
    IOptions<NpgmqHostingOptions> optionsWrapper,
    ILogger<NpgmqBackgroundService> logger
) : BackgroundService,
    IHostedLifecycleService
{
    private readonly NpgmqHostingOptions _options = optionsWrapper.Value;

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        await client.InitAsync(cancellationToken);

        var version = await client.GetPgmqVersionAsync(cancellationToken);

        logger.LogInformation("Npgmq version: {version}", version);

        await RegisterQueuesAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tasks = _options.QueueOptions
            .SelectMany(queue =>
                queue.Handlers.Select(handler => PollQueueForMessageAsync(queue, handler, stoppingToken)));

        await Task.WhenAll(tasks);
    }

    private async Task PollQueueForMessageAsync(
        NpgmqHostingQueueOptions queue,
        INpgmqHostingQueueHandlerOptions handler,
        CancellationToken cancellationToken
    )
    {
        logger.LogInformation("Started Npgmq queue handler for queue {queue}", queue.QueueName);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await handler.PollBatchAsync(client, serviceScopeFactory, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed to process Npgmq queue {queue}", queue.QueueName);

                await Task.Delay(queue.ErrorDelay, cancellationToken);
            }
        }
    }

    async ValueTask RegisterQueuesAsync(CancellationToken cancellationToken)
    {
        foreach (var queue in _options.QueueOptions)
        {
            await client.CreateQueueAsync(queue.QueueName, cancellationToken);

            logger.LogInformation("Registered Npgmq queue: {queue}", queue.QueueName);
        }
    }

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}