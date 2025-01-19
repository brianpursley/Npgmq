namespace Npgmq;

/// <summary>
/// PGMQ client.
/// </summary>
public interface INpgmqClient
{
    /// <summary>
    /// Archive a message.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgId">The ID of the message to archive.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if the message was archived, false otherwise.</returns>
    Task<bool> ArchiveAsync(string queueName, long msgId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Archive multiple messages.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgIds">The IDs of the messages to archive.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>List of IDs that were archived.</returns>
    Task<List<long>> ArchiveBatchAsync(string queueName, IEnumerable<long> msgIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Create a new queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CreateQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new unlogged queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CreateUnloggedQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete a message.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgId">The ID of the message to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if the message was deleted, false otherwise.</returns>
    Task<bool> DeleteAsync(string queueName, long msgId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes multiple messages.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgIds">The IDs of the messages to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>List of IDs that were deleted.</returns>
    Task<List<long>> DeleteBatchAsync(string queueName, IEnumerable<long> msgIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drop a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task DropQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Create pgmq extension, if it does not exist.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task InitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the version of the pgmq extension installed in the database.
    /// </summary>
    /// <remarks>
    /// This method will return null if the pgmq extension is not installed.
    /// </remarks>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A <see cref="Version" /> object representing the version of the pgmq extension.</returns>
    Task<Version?> GetPgmqVersionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// List queues.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The list of queues.</returns>
    Task<List<NpgmqQueue>> ListQueuesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Poll a queue for a message.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="pollTimeoutSeconds">The amount of time to poll for, in seconds.</param>
    /// <param name="pollIntervalMilliseconds">The amount of time to wait between polls, in milliseconds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The message read, or null if no message was read.</returns>
    Task<NpgmqMessage<T>?> PollAsync<T>(string queue, int vt = NpgmqClient.DefaultVt, int pollTimeoutSeconds = NpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = NpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Poll a queue for multiple messages.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="pollTimeoutSeconds">The amount of time to poll for, in seconds.</param>
    /// <param name="pollIntervalMilliseconds">The amount of time to wait between polls, in milliseconds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> PollBatchAsync<T>(string queueName, int vt = NpgmqClient.DefaultVt, int limit = NpgmqClient.DefaultReadBatchLimit, int pollTimeoutSeconds = NpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = NpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Read a message from a queue and immediately delete it.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The message read, or null if no message was read.</returns>
    Task<NpgmqMessage<T>?> PopAsync<T>(string queueName, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Purge a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of messages purged.</returns>
    Task<long> PurgeQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether a queue exists or not.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if the queue exists, false otherwise.</returns>
    Task<bool> QueueExistsAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Read a message from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The message read, or null if no message was read.</returns>
    Task<NpgmqMessage<T>?> ReadAsync<T>(string queue, int vt = NpgmqClient.DefaultVt, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Read multiple messages from a queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> ReadBatchAsync<T>(string queue, int vt = NpgmqClient.DefaultVt, int limit = NpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message to a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The ID of the sent message.</returns>
    Task<long> SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message to a queue, visible after a specified number of seconds.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The ID of the sent message.</returns>
    Task<long> SendAsync<T>(string queueName, T message, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message to a queue with a delayed vt.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The ID of the sent message.</returns>
    [Obsolete("Use SendAsync instead.")]
    Task<long> SendDelayAsync<T>(string queueName, T message, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send multiple messages to a queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The IDs of the sent messages.</returns>
    Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send multiple messages to a queue, visible after a specified number of seconds.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The IDs of the sent messages.</returns>
    Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Adjust the Vt of an existing message.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="msgId">The message ID.</param>
    /// <param name="vtOffset">The number of seconds to be added to the current Vt.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task SetVtAsync(string queueName, long msgId, int vtOffset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get metrics for all queues.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of <see cref="NpgmqMetricsResult" /></returns>
    Task<List<NpgmqMetricsResult>> GetMetricsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get metrics for a specific queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An <see cref="NpgmqMetricsResult" /></returns>
    Task<NpgmqMetricsResult> GetMetricsAsync(string queueName, CancellationToken cancellationToken = default);
}