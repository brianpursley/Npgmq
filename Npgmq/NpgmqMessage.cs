namespace Npgmq;

/// <summary>
/// PGMQ message.
/// </summary>
/// <typeparam name="T">The type of the value contained within this message.</typeparam>
public class NpgmqMessage<T>
{
    /// <summary>
    /// Unique identifier for the message.
    /// </summary>
    public long MsgId { get; set; }
    
    /// <summary>
    /// The number of times the message has been read. Increments on read.
    /// </summary>
    public int ReadCt { get; set; }
    
    /// <summary>
    /// UTC timestamp at which the message was sent to the queue.
    /// </summary>
    public DateTime EnqueuedAt { get; set; }

    /// <summary>
    /// UTC timestamp at which the message will be available for reading again.
    /// </summary>
    public DateTime Vt { get; set; }
    
    /// <summary>
    /// The message value.
    /// </summary>
    public T? Message { get; set; }
}