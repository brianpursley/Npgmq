namespace Npgmq;

/// <summary>
/// PGMQ client.
/// </summary>
public interface INpgmqClient
{
    public const int DefaultPartitionInterval = 10_000;
    public const int DefaultRetentionInterval = 100_000;
    public const int DefaultVt = 30;
    public const int DefaultReadBatchLimit = 10;
    public const int DefaultPollTimeoutSeconds = 5;
    public const int DefaultPollIntervalMilliseconds = 250;
    
    /// <summary>
    /// Archive a message.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgId">The ID of the message to archive.</param>
    /// <returns>True if the message was archived, false otherwise.</returns>
    Task<bool> ArchiveAsync(string queueName, long msgId);

    /// <summary>
    /// Archive multiple messages.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgIds">The IDs of the messages to archive.</param>
    /// <returns>List of IDs that were archived.</returns>
    Task<List<long>> ArchiveBatchAsync(string queueName, IEnumerable<long> msgIds);

    /// <summary>
    /// Create a new partitioned queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="partitionInterval">The partition interval in seconds.</param>
    /// <param name="retentionInterval">The retention interval in seconds.</param>
    Task CreatePartitionedQueueAsync(string queueName, int partitionInterval = DefaultPartitionInterval, int retentionInterval = DefaultRetentionInterval);

    /// <summary>
    /// Create a new queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    Task CreateQueueAsync(string queueName);
    
    /// <summary>
    /// Delete a message.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgId">The ID of the message to delete.</param>
    /// <returns>True if the message was deleted, false otherwise.</returns>
    Task<bool> DeleteAsync(string queueName, long msgId);

    /// <summary>
    /// Deletes multiple messages.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgIds">The IDs of the messages to delete.</param>
    /// <returns>List of IDs that were deleted.</returns>
    Task<List<long>> DeleteBatchAsync(string queueName, IEnumerable<long> msgIds);

    /// <summary>
    /// Drop a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="partitioned">Whether the queue is partitioned.</param>
    Task DropQueueAsync(string queueName, bool partitioned = false);

    /// <summary>
    /// List queues.
    /// </summary>
    /// <returns>The list of queues.</returns>
    Task<List<NpgmqQueue>> ListQueuesAsync();
    
    /// <summary>
    /// Poll a queue for a message.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="pollTimeoutSeconds">The amount of time to poll for, in seconds.</param>
    /// <param name="pollIntervalMilliseconds">The amount of time to wait between polls, in milliseconds.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The message read, or null if no message was read.</returns>
    Task<NpgmqMessage<T>?> PollAsync<T>(string queue, int vt = DefaultVt, int pollTimeoutSeconds = DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = DefaultPollIntervalMilliseconds);
    
    /// <summary>
    /// Poll a queue for multiple messages.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="pollTimeoutSeconds">The amount of time to poll for, in seconds.</param>
    /// <param name="pollIntervalMilliseconds">The amount of time to wait between polls, in milliseconds.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> PollBatchAsync<T>(string queueName, int vt = DefaultVt, int limit = DefaultReadBatchLimit, int pollTimeoutSeconds = DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = DefaultPollIntervalMilliseconds);

    /// <summary>
    /// Read a message from a queue and immediately delete it.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The message read, or null if no message was read.</returns>
    Task<NpgmqMessage<T>?> PopAsync<T>(string queueName);
    
    /// <summary>
    /// Purge a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <returns>The number of messages purged.</returns>
    Task<long> PurgeQueueAsync(string queueName);

    /// <summary>
    /// Checks whether a queue exists or not.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <returns>True if the queue exists, false otherwise.</returns>
    Task<bool> QueueExistsAsync(string queueName);
    
    /// <summary>
    /// Read a message from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The message read, or null if no message was read.</returns>
    Task<NpgmqMessage<T>?> ReadAsync<T>(string queue, int vt = DefaultVt);

    /// <summary>
    /// Read multiple messages from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> ReadBatchAsync<T>(string queue, int vt = DefaultVt, int limit = DefaultReadBatchLimit);
    
    /// <summary>
    /// Send a message to a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="message">The message to send.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The ID of the sent message.</returns>
    Task<long> SendAsync<T>(string queueName, T message) where T : class;

    /// <summary>
    /// Send multiple messages to a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="messages">The messages to send.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The IDs of the sent messages.</returns>
    Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages) where T : class;

    /// <summary>
    /// Adjust the Vt of an existing message.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgId">The message ID.</param>
    /// <param name="vtOffset">The number of seconds to be added to the current Vt.</param>
    Task SetVtAsync(string queueName, long msgId, int vtOffset);
}