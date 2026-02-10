using System.Data;
using System.Data.Common;
using System.Text.Json;
using JOS.SystemTextJson.DictionaryStringObject.JsonConverter;
using Npgsql;
using NpgsqlTypes;

namespace Npgmq;

/// <inheritdoc cref="INpgmqClient" />
public class NpgmqClient : INpgmqClient
{
    // Specialized JsonSerializerOptions to handle Dictionary<string, object> deserialization
    private static readonly JsonSerializerOptions DeserializationOptions = new()
    {
        Converters = { new DictionaryStringObjectJsonConverter() }
    };

    private readonly NpgmqCommandFactory _commandFactory;

    /// <summary>
    /// Create a new <see cref="NpgmqClient"/> using a connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public NpgmqClient(string connectionString)
    {
        _commandFactory = new NpgmqCommandFactory(connectionString);
    }

    /// <summary>
    /// Create a new <see cref="NpgmqClient"/> using an <see cref="NpgsqlDataSource"/>.
    /// </summary>
    /// <param name="dataSource">The data source to use.</param>
    public NpgmqClient(NpgsqlDataSource dataSource)
    {
        _commandFactory = new NpgmqCommandFactory(dataSource);
    }

    /// <summary>
    /// Create a new <see cref="NpgmqClient"/> using an existing <see cref="NpgsqlConnection"/>.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <remarks>
    /// The client will ensure the connection is open before executing commands.
    /// The provided connection will not be disposed by the client. It is the caller's responsibility to manage the connection's lifecycle.
    /// You are responsible for managing connection lifetime and handling concurrent usage if the same connection is used across multiple threads.
    /// If you are concerned about connection management or concurrency, consider using a connection string or an <see cref="NpgsqlDataSource"/> instead.
    /// </remarks>
    public NpgmqClient(NpgsqlConnection connection)
    {
        _commandFactory = new NpgmqCommandFactory(connection);
    }

    public async Task<bool> ArchiveAsync(string queueName, long msgId, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.archive(@queue_name, @msg_id);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result is not null && Convert.ToBoolean(result);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to archive message {msgId} in queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> ArchiveBatchAsync(string queueName, IEnumerable<long> msgIds, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.archive(@queue_name, @msg_ids);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint, msgIds.ToArray());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to archive messages in queue {queueName}.", ex);
        }
    }

    public async Task CreateQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.create(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to create queue {queueName}.", ex);
        }
    }

    public async Task CreateUnloggedQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.create_unlogged(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to create unlogged queue {queueName}.", ex);
        }
    }

    public async Task<bool> DeleteAsync(string queueName, long msgId, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.delete(@queue_name, @msg_id);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result is not null && Convert.ToBoolean(result);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to delete message {msgId} from queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> DeleteBatchAsync(string queueName, IEnumerable<long> msgIds, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.delete(@queue_name, @msg_ids);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint, msgIds.ToArray());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to delete messages from queue {queueName}.", ex);
        }
    }

    public async Task<bool> DropQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.drop_queue(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result is not null && Convert.ToBoolean(result);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to drop queue {queueName}.", ex);
        }
    }

    public async Task InitAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException("Failed to initialize PGMQ extension.", ex);
        }
    }

    public async Task<Version?> GetPgmqVersionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT extversion FROM pg_extension WHERE extname = 'pgmq';", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result switch
                {
                    string versionString => new Version(versionString),
                    null => null,
                    _ => throw new NpgmqException($"Unexpected result type: {result.GetType().Name}")
                };
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException("Failed to get PGMQ version.", ex);
        }
    }

    public async Task<List<NpgmqQueue>> ListQueuesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory
                .CreateAsync("SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues();",
                    cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<NpgmqQueue>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(new NpgmqQueue
                        {
                            QueueName = reader.GetString(0),
                            CreatedAt = reader.GetFieldValue<DateTimeOffset>(1),
                            IsPartitioned = reader.GetBoolean(2),
                            IsUnlogged = reader.GetBoolean(3)
                        });
                    }

                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException("Failed to list queues.", ex);
        }
    }

    public async Task<NpgmqMessage<T>?> PollAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int pollTimeoutSeconds = INpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = INpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var result = await PollBatchAsync<T>(queueName, vt, 1, pollTimeoutSeconds, pollIntervalMilliseconds, cancellationToken).ConfigureAwait(false);
            return result.SingleOrDefault();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to poll queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> PollBatchAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, int pollTimeoutSeconds = INpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = INpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_with_poll(@queue_name, @vt, @qty, @poll_timeout_s, @poll_interval_ms);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@vt", vt);
                cmd.Parameters.AddWithValue("@qty", limit);
                cmd.Parameters.AddWithValue("@poll_timeout_s", pollTimeoutSeconds);
                cmd.Parameters.AddWithValue("@poll_interval_ms", pollIntervalMilliseconds);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to poll queue {queueName}.", ex);
        }
    }

    public async Task<NpgmqMessage<T>?> PopAsync<T>(string queueName, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.pop(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = await ReadMessagesAsync<T>(reader, cancellationToken).ConfigureAwait(false);
                    return result.SingleOrDefault();
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to pop queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> PopAsync<T>(string queueName, int qty, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.pop(@queue_name, @qty);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@qty", qty);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to pop queue {queueName}.", ex);
        }
    }

    public async Task<long> PurgeQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.purge_queue(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to purge queue {queueName}.", ex);
        }
    }

    public async Task<bool> QueueExistsAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT 1 WHERE EXISTS (SELECT * FROM pgmq.list_queues() WHERE queue_name = @queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt32(result ?? 0) == 1;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to check if queue {queueName} exists.", ex);
        }
    }

    public async Task<NpgmqMessage<T>?> ReadAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var result = await ReadBatchAsync<T>(queueName, vt, 1, cancellationToken).ConfigureAwait(false);
            return result.SingleOrDefault();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> ReadBatchAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read(@queue_name, @vt, @qty);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@vt", vt);
                cmd.Parameters.AddWithValue("@qty", limit);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @msg);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message, IReadOnlyDictionary<string, object> headers, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @msg, @headers);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Jsonb, SerializeHeaders(headers));
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @msg, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@delay", delay);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @msg, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@delay", delay.ToUniversalTime());
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message, IReadOnlyDictionary<string, object> headers, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @msg, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Jsonb, SerializeHeaders(headers));
                cmd.Parameters.AddWithValue("@delay", delay);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message, IReadOnlyDictionary<string, object> headers, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @msg, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Jsonb, SerializeHeaders(headers));
                cmd.Parameters.AddWithValue("@delay", delay.ToUniversalTime());
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @msgs);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IReadOnlyList<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @msgs, @headers);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, headers.Select(SerializeHeaders).ToArray());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @msgs, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @msgs, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay.ToUniversalTime());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IReadOnlyList<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @msgs, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, headers.Select(SerializeHeaders).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IReadOnlyList<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @msgs, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, headers.Select(SerializeHeaders).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay.ToUniversalTime());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task SetVtAsync(string queueName, long msgId, int vtOffset, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.set_vt(@queue_name, @msg_id, @vt);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                cmd.Parameters.AddWithValue("@vt", vtOffset);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to set VT for message {msgId} in queue {queueName}.", ex);
        }
    }

    public async Task SetVtAsync(string queueName, long msgId, DateTimeOffset vt, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.set_vt(@queue_name, @msg_id, @vt);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                cmd.Parameters.AddWithValue("@vt", vt.ToUniversalTime());
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to set VT for message {msgId} in queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SetVtBatchAsync(string queueName, IEnumerable<long> msgIds, int vtOffset, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id FROM pgmq.set_vt(@queue_name, @msg_ids, @vt);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint, msgIds.ToArray());
                cmd.Parameters.AddWithValue("@vt", vtOffset);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to set VT for messages in queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SetVtBatchAsync(string queueName, IEnumerable<long> msgIds, DateTimeOffset vt, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id FROM pgmq.set_vt(@queue_name, @msg_ids, @vt);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint, msgIds.ToArray());
                cmd.Parameters.AddWithValue("@vt", vt.ToUniversalTime());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to set VT for messages in queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMetricsResult>> GetMetricsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time FROM pgmq.metrics_all();", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMetricsAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException("Failed to get metrics.", ex);
        }
    }

    public async Task<NpgmqMetricsResult> GetMetricsAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time FROM pgmq.metrics(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var results = await ReadMetricsAsync(reader, cancellationToken).ConfigureAwait(false);
                    return results.Count switch
                    {
                        1 => results.Single(),
                        0 => throw new NpgmqException("No data returned."),
                        _ => throw new NpgmqException("Multiple results returned.")
                    };
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to get metrics for queue {queueName}.", ex);
        }
    }

    private static async Task<List<NpgmqMetricsResult>> ReadMetricsAsync(DbDataReader reader, CancellationToken cancellationToken = default)
    {
        var results = new List<NpgmqMetricsResult>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            results.Add(new NpgmqMetricsResult
            {
                QueueName = reader.GetString(0),
                QueueLength = reader.GetInt64(1),
                NewestMessageAge = await reader.IsDBNullAsync(2, cancellationToken).ConfigureAwait(false) ? null : reader.GetInt32(2),
                OldestMessageAge = await reader.IsDBNullAsync(3, cancellationToken).ConfigureAwait(false) ? null : reader.GetInt32(3),
                TotalMessages = reader.GetInt64(4),
                ScrapeTime = reader.GetFieldValue<DateTimeOffset>(5)
            });
        }
        return results;
    }

    private static async Task<List<NpgmqMessage<T>>> ReadMessagesAsync<T>(DbDataReader reader, CancellationToken cancellationToken = default) where T : class
    {
        var result = new List<NpgmqMessage<T>>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(new NpgmqMessage<T>
            {
                MsgId = reader.GetInt64(0),
                ReadCt = reader.GetInt32(1),
                EnqueuedAt = reader.GetFieldValue<DateTimeOffset>(2),
                Vt = reader.GetFieldValue<DateTimeOffset>(3),
                Message = await reader.IsDBNullAsync(4, cancellationToken).ConfigureAwait(false) ? null : DeserializeMessage<T>(reader.GetFieldValue<string>(4)),
                Headers = await reader.IsDBNullAsync(5, cancellationToken).ConfigureAwait(false) ? null : DeserializeHeaders(reader.GetFieldValue<string>(5))
            });
        }
        return result;
    }

    private static string SerializeMessage<T>(T message) where T : class =>
        typeof(T) == typeof(string) ? message as string ?? "" : JsonSerializer.Serialize(message);

    private static T? DeserializeMessage<T>(string message) where T : class =>
        typeof(T) == typeof(string) ? (T?)(object?)message : JsonSerializer.Deserialize<T?>(message, DeserializationOptions);

    private static string SerializeHeaders(IReadOnlyDictionary<string, object> headers) =>
        JsonSerializer.Serialize(headers);

    private static Dictionary<string, object>? DeserializeHeaders(string headers) =>
        string.IsNullOrEmpty(headers) ? null : JsonSerializer.Deserialize<Dictionary<string, object>>(headers, DeserializationOptions);
}