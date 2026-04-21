namespace Npgmq;

public partial interface INpgmqClient
{
    /// <summary>
    /// Poll a queue for multiple grouped messages.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.9.0 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="pollTimeoutSeconds">The amount of time to poll for, in seconds.</param>
    /// <param name="pollIntervalMilliseconds">The amount of time to wait between polls, in milliseconds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> PollGroupedAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, int pollTimeoutSeconds = INpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = INpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Poll a queue for multiple grouped messages using round-robin group selection.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.9.0 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="pollTimeoutSeconds">The amount of time to poll for, in seconds.</param>
    /// <param name="pollIntervalMilliseconds">The amount of time to wait between polls, in milliseconds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> PollGroupedRoundRobinAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, int pollTimeoutSeconds = INpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = INpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Read multiple grouped messages from queue.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.9.0 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> ReadGroupedAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Read multiple grouped messages from queue using round-robin group selection.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.9.0 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> ReadGroupedRoundRobinAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Read multiple grouped head messages from queue.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.1 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="vt">The visibility time in seconds.</param>
    /// <param name="limit">The maximum number of messages to read.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>The messages read.</returns>
    Task<List<NpgmqMessage<T>>> ReadGroupedHeadAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Creates an index on a queue's table header column to optimize grouped message reads.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.9.0 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CreateFifoIndexAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates indexes on all queue table header columns to optimize grouped message reads.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.9.0 or later.
    /// </remarks>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CreateFifoIndexesAsync(CancellationToken cancellationToken = default);
}