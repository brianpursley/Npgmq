namespace Npgmq;

public partial interface INpgmqClient
{
    /// <summary>
    /// Binds a topic pattern to a queue. Messages sent to the topic with a routing key that matches the pattern will be delivered to the queue.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="pattern">The topic pattern to bind.</param>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task BindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Unbinds a topic pattern from a queue.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="pattern">The topic pattern to unbind.</param>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if the binding was successfully removed, false if the binding did not exist.</returns>
    Task<bool> UnbindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all topic bindings.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>List of topic bindings.</returns>
    Task<List<NpgmqTopicBinding>> ListTopicBindingsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all topic bindings for a specified queue.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="queueName">The queue name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>List of topic bindings.</returns>
    Task<List<NpgmqTopicBinding>> ListTopicBindingsAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>Number of messages sent.</returns>
    Task<int> SendTopicAsync<T>(string routingKey, T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends a message to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>Number of messages sent.</returns>
    Task<int> SendTopicAsync<T>(string routingKey, T message, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends a message to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>Number of messages sent.</returns>
    Task<int> SendTopicAsync<T>(string routingKey, T message, IReadOnlyDictionary<string, object> headers, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends messages to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>List of messages sent.</returns>
    Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends messages to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>List of messages sent.</returns>
    Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends messages to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>List of messages sent.</returns>
    Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends messages to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="delay">Time at which the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>List of messages sent.</returns>
    Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends messages to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="delay">Number of seconds until the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>List of messages sent.</returns>
    Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delay, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Sends messages to the topic with the specified routing key.
    /// </summary>
    /// <remarks>
    /// Requires pgmq 1.11.0 or later.
    /// </remarks>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="messages">The messages to send.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="delay">Time at which the message becomes visible.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>List of messages sent.</returns>
    Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class;
}