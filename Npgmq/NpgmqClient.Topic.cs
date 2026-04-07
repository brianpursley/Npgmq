using System.Data.Common;
using NpgsqlTypes;

namespace Npgmq;

public partial class NpgmqClient
{
    public async Task BindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.bind_topic(@pattern, @queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@pattern", pattern);
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to bind topic to queue {queueName}.", ex);
        }
    }

    public async Task<bool> UnbindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.unbind_topic(@pattern, @queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@pattern", pattern);
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return result is not null && Convert.ToBoolean(result);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to unbind topic from queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqTopicBinding>> ListTopicBindingsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings();", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadTopicBindingsAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to list topic bindings.", ex);
        }
    }

    public async Task<List<NpgmqTopicBinding>> ListTopicBindingsAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadTopicBindingsAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to list topic bindings for queue {queueName}.", ex);
        }
    }

    public async Task<int> SendTopicAsync<T>(string routingKey, T message, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_topic(@routing_key, @msg);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt32(result);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send message to topic.", ex);
        }
    }

    public async Task<int> SendTopicAsync<T>(string routingKey, T message, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_topic(@routing_key, @msg, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@delay", delay);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt32(result);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send message to topic.", ex);
        }
    }

    public async Task<int> SendTopicAsync<T>(string routingKey, T message, IReadOnlyDictionary<string, object> headers, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_topic(@routing_key, @msg, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msg", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Jsonb, SerializeHeaders(headers));
                cmd.Parameters.AddWithValue("@delay", delay);
                var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return Convert.ToInt32(result);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send message to topic.", ex);
        }
    }

    public async Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, msg_id FROM pgmq.send_batch_topic(@routing_key, @msgs);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadSendTopicResponsesAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send messages to topic.", ex);
        }
    }

    public async Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, msg_id FROM pgmq.send_batch_topic(@routing_key, @msgs, @headers);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, headers.Select(SerializeHeaders).ToArray());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadSendTopicResponsesAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send messages to topic.", ex);
        }
    }

    public async Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, msg_id FROM pgmq.send_batch_topic(@routing_key, @msgs, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadSendTopicResponsesAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send messages to topic.", ex);
        }
    }

    public async Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, msg_id FROM pgmq.send_batch_topic(@routing_key, @msgs, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay.ToUniversalTime());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadSendTopicResponsesAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send messages to topic.", ex);
        }
    }

    public async Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, msg_id FROM pgmq.send_batch_topic(@routing_key, @msgs, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, headers.Select(SerializeHeaders).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadSendTopicResponsesAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send messages to topic.", ex);
        }
    }

    public async Task<List<NpgmqSendTopicResult>> SendBatchTopicAsync<T>(string routingKey, IEnumerable<T> messages, IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset delay, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, msg_id FROM pgmq.send_batch_topic(@routing_key, @msgs, @headers, @delay);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@routing_key", routingKey);
                cmd.Parameters.AddWithValue("@msgs", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                cmd.Parameters.AddWithValue("@headers", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, headers.Select(SerializeHeaders).ToArray());
                cmd.Parameters.AddWithValue("@delay", delay.ToUniversalTime());
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadSendTopicResponsesAsync(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to send messages to topic.", ex);
        }
    }

    private static async Task<List<NpgmqTopicBinding>> ReadTopicBindingsAsync(DbDataReader reader, CancellationToken cancellationToken = default)
    {
        var result = new List<NpgmqTopicBinding>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(new NpgmqTopicBinding
            {
                Pattern = reader.GetString(0),
                QueueName = reader.GetString(1),
                BoundAt = reader.GetFieldValue<DateTimeOffset>(2),
                CompiledRegex = reader.GetString(3)
            });
        }
        return result;
    }

    private static async Task<List<NpgmqSendTopicResult>> ReadSendTopicResponsesAsync(DbDataReader reader, CancellationToken cancellationToken = default)
    {
        var result = new List<NpgmqSendTopicResult>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(new NpgmqSendTopicResult
            {
                QueueName = reader.GetString(0),
                MsgId = reader.GetInt64(1)
            });
        }
        return result;
    }
}