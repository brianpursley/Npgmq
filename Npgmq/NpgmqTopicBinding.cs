namespace Npgmq;

/// <summary>
/// PGMQ Topic binding.
/// </summary>
public class NpgmqTopicBinding
{
    /// <summary>
    /// Wildcard pattern for routing key matching.
    /// </summary>
    public string Pattern { get; set; } = null!;

    /// <summary>
    /// Name of the queue that receives messages when pattern matches.
    /// </summary>
    public string QueueName { get; set; } = null!;

    /// <summary>
    /// Timestamp when the binding was created.
    /// </summary>
    public DateTimeOffset BoundAt { get; set; }

    /// <summary>
    /// Compiled regex for the wildcard pattern.
    /// </summary>
    public string CompiledRegex { get; set; } = null!;
}