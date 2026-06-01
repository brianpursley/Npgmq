using Microsoft.Extensions.Logging;

namespace Npgmq.Example.Hosting;

public sealed class SampleEventHandler(ILogger<SampleEventHandler> logger) : IConsumer<SampleEvent>
{
    public Task ConsumeAsync(NpgmqMessage<SampleEvent> message, CancellationToken cancellationToken)
    {
        logger.LogInformation("Sample event received - {MessageId}", message.MsgId);
        
        return Task.CompletedTask;
    }
}
