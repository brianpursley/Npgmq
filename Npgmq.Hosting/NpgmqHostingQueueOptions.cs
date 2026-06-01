namespace Npgmq;

public sealed class NpgmqHostingQueueOptions(string queueName)
{
    public string QueueName { get; } = queueName;

    internal readonly List<INpgmqHostingQueueHandlerOptions> Handlers = [];

    public int VisibilityTimeout { get; private set; } = INpgmqClient.DefaultVt;

    public int BatchSize { get; private set; } = INpgmqClient.DefaultReadBatchLimit;

    public int PollTimeoutSeconds { get; private set; } = INpgmqClient.DefaultPollTimeoutSeconds;

    public int PollIntervalMilliseconds { get; private set; } = INpgmqClient.DefaultPollIntervalMilliseconds;

    public TimeSpan ErrorDelay { get; private set; } = TimeSpan.FromSeconds(5);

    public NpgmqHostingQueueOptions WithHandler<TMessage, THandler>()
        where TMessage : class
        where THandler : class, IConsumer<TMessage>
    {
        Handlers.Add(new NpgmqHostingQueueHandlerOptions<TMessage, THandler>(this));
        return this;
    }

    public NpgmqHostingQueueOptions WithVisibilityTimeout(int seconds)
    {
        VisibilityTimeout = seconds;
        return this;
    }

    public NpgmqHostingQueueOptions WithBatchSize(int batchSize)
    {
        BatchSize = batchSize;
        return this;
    }

    public NpgmqHostingQueueOptions WithPollTimeout(int seconds)
    {
        PollTimeoutSeconds = seconds;
        return this;
    }

    public NpgmqHostingQueueOptions WithPollInterval(int milliseconds)
    {
        PollIntervalMilliseconds = milliseconds;
        return this;
    }

    public NpgmqHostingQueueOptions WithErrorDelay(TimeSpan delay)
    {
        ErrorDelay = delay;
        return this;
    }
}