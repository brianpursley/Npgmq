namespace Npgmq;

/// <summary>
/// PGMQ Queue.
/// </summary>
public class NpgmqQueue
{
    /// <summary>
    /// The name of the queue.
    /// </summary>
    public string QueueName { get; set; } = null!;
    
    /// <summary>
    /// UTC timestamp at which the queue was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }
}