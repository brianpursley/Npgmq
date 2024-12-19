namespace Npgmq;

/// <summary>
/// Metrics data for a queue.
/// </summary>
public class NpgmqMetricsResult
{
    /// <summary>
    /// Name of the queue.
    /// </summary>
    public string QueueName { get; set; } = null!;
    
    /// <summary>
    /// Number of messages in the queue.
    /// </summary>
    public long QueueLength { get; set; }
    
    /// <summary>
    /// Age, in seconds, of the newest message in the queue, or null if the queue is empty.
    /// </summary>
    public int? NewestMessageAge { get; set; }
    
    /// <summary>
    /// Age, in seconds, of the oldest message in the queue, or null if the queue is empty.
    /// </summary>
    public int? OldestMessageAge { get; set; }
    
    /// <summary>
    /// Total number of messages that have been in the queue.
    /// </summary>
    public long TotalMessages { get; set; }
    
    /// <summary>
    /// When the metrics were scraped.
    /// </summary>
    public DateTimeOffset ScrapeTime { get; set; }
}