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
    /// Timestamp at which the queue was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }
    
    /// <summary>
    /// Whether the queue is partitioned.
    /// </summary>
    public bool IsPartitioned { get; set; }
    
    /// <summary>
    /// Whether the queue is unlogged.
    /// </summary>
    public bool IsUnlogged { get; set; }
}