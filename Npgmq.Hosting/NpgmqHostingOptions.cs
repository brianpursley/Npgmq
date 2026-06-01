namespace Npgmq;

public sealed class NpgmqHostingOptions
{
    private readonly Dictionary<string, NpgmqHostingQueueOptions> _queueOptions = [];

    internal IEnumerable<NpgmqHostingQueueOptions> QueueOptions => _queueOptions.Values;

    public NpgmqHostingOptions UseQueue(string queueName, Action<NpgmqHostingQueueOptions> configure)
    {
        var options = GetQueueOptions(queueName);

        configure(options);

        return this;
    }

    private NpgmqHostingQueueOptions GetQueueOptions(string queueName)
    {
        if (_queueOptions.TryGetValue(queueName, out NpgmqHostingQueueOptions? value))
        {
            return value;
        }

        value = new NpgmqHostingQueueOptions(queueName);

        _queueOptions.Add(queueName, value);

        return value;
    }
}