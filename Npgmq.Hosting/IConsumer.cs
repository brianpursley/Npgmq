namespace Npgmq;

public interface IConsumer<TMessage>
    where TMessage : class
{
    Task ConsumeAsync(NpgmqMessage<TMessage> message, CancellationToken cancellationToken);
}
