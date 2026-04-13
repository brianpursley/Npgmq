namespace Npgmq;

/// <summary>
/// Result of sending a message to a topic.
/// </summary>
public class NpgmqSendTopicResult
{
    /// <summary>
    /// The queue name where the message was sent.
    /// </summary>
    public string QueueName { get; set; } = null!;

    /// <summary>
    /// Unique identifier for the message.
    /// </summary>
    public long MsgId { get; set; }
}