using System.Data.Common;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Npgmq;

/// <inheritdoc cref="INpgmqClient" />
public class NpgmqClient : INpgmqClient
{
    public const int DefaultVt = 30;
    public const int DefaultReadBatchLimit = 10;
    public const int DefaultPollTimeoutSeconds = 5;
    public const int DefaultPollIntervalMilliseconds = 250;
    
    private readonly NpgsqlConnection _connection;

    /// <summary>
    /// Create a new PGMQ client.
    /// </summary>
    /// <param name="connection">The connection <see cref="NpgsqlConnection" />.</param>
    public NpgmqClient(NpgsqlConnection connection)
    {
        _connection = connection;
    }

    public async Task<bool> ArchiveAsync(string queueName, long msgId)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.archive(@queue_name, @msg_id);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("@queue_name", queueName);
            cmd.Parameters.AddWithValue("@msg_id", msgId);
            var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return (bool)result!;
        }
    }

    public async Task<List<long>> ArchiveBatchAsync(string queueName, IEnumerable<long> msgIds)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.archive(@queue_name, @msg_ids);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("@queue_name", queueName);
            cmd.Parameters.AddWithValue("@msg_ids", msgIds);
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                var result = new List<long>();
                while (await reader.ReadAsync().ConfigureAwait(false)) result.Add(reader.GetInt64(0));

                return result;
            }
        }
    }

    public async Task CreateQueueAsync(string queueName)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.create(@queue_name);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    public async Task CreateUnloggedQueueAsync(string queueName)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.create_unlogged(@queue_name);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    public async Task<bool> DeleteAsync(string queueName, long msgId)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.delete(@queue_name, @msg_id);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("@queue_name", queueName);
            cmd.Parameters.AddWithValue("@msg_id", msgId);
            var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return (bool)result!;
        }
    }

    public async Task<List<long>> DeleteBatchAsync(string queueName, IEnumerable<long> msgIds)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.delete(@queue_name, @msg_ids);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("@queue_name", queueName);
            cmd.Parameters.AddWithValue("@msg_ids", msgIds.ToArray());
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                var result = new List<long>();
                while (await reader.ReadAsync().ConfigureAwait(false)) result.Add(reader.GetInt64(0));

                return result;
            }
        }
    }

    public async Task DropQueueAsync(string queueName)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.drop_queue(@queue_name);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    public async Task InitAsync()
    {
        var cmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
    
    public async Task<List<NpgmqQueue>> ListQueuesAsync()
    {
        var cmd = new NpgsqlCommand("SELECT * FROM pgmq.list_queues();", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                var result = new List<NpgmqQueue>();
                while (await reader.ReadAsync().ConfigureAwait(false))
                    result.Add(new NpgmqQueue
                    {
                        QueueName = reader.GetString(0),
                        CreatedAt = reader.GetDateTime(1)
                    });

                return result;
            }
        }
    }

    public async Task<NpgmqMessage<T>?> PollAsync<T>(string queueName, int vt = DefaultVt, int pollTimeoutSeconds = DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = DefaultPollIntervalMilliseconds) where T : class
    {
        var result = await PollBatchAsync<T>(queueName, vt, 1, pollTimeoutSeconds, pollIntervalMilliseconds).ConfigureAwait(false);
        return result.SingleOrDefault();
    }

    public async Task<List<NpgmqMessage<T>>> PollBatchAsync<T>(string queueName, int vt = DefaultVt, int limit = DefaultReadBatchLimit, int pollTimeoutSeconds = DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = DefaultPollIntervalMilliseconds) where T : class
    {
        var cmd = new NpgsqlCommand("SELECT * FROM pgmq.read_with_poll(@queue_name, @vt, @limit, @poll_timeout_s, @poll_interval_ms);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            cmd.Parameters.AddWithValue("vt", vt);
            cmd.Parameters.AddWithValue("limit", limit);
            cmd.Parameters.AddWithValue("poll_timeout_s", pollTimeoutSeconds);
            cmd.Parameters.AddWithValue("poll_interval_ms", pollIntervalMilliseconds);
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                return await ReadMessagesAsync<T>(reader).ConfigureAwait(false);
            }
        }
    }

    public async Task<NpgmqMessage<T>?> PopAsync<T>(string queueName) where T : class
    {
        var cmd = new NpgsqlCommand("SELECT * FROM pgmq.pop(@queue_name);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                var result = await ReadMessagesAsync<T>(reader).ConfigureAwait(false);
                return result.SingleOrDefault();
            }
        }
    }

    public async Task<long> PurgeQueueAsync(string queueName)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.purge_queue(@queue_name);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return (long)result!;
        }
    }

    public async Task<bool> QueueExistsAsync(string queueName)
    {
        var cmd = new NpgsqlCommand("SELECT 1 WHERE EXISTS (SELECT * FROM pgmq.list_queues() WHERE queue_name = @queue_name);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return (int)(result ?? 0) == 1;
        }
    }

    public async Task<NpgmqMessage<T>?> ReadAsync<T>(string queueName, int vt = DefaultVt) where T : class
    {
        var result = await ReadBatchAsync<T>(queueName, vt, 1).ConfigureAwait(false);
        return result.SingleOrDefault();
    }

    public async Task<List<NpgmqMessage<T>>> ReadBatchAsync<T>(string queueName, int vt = DefaultVt, int limit = DefaultReadBatchLimit) where T : class
    {
        var cmd = new NpgsqlCommand("SELECT * FROM pgmq.read(@queue_name, @vt, @limit);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            cmd.Parameters.AddWithValue("vt", vt);
            cmd.Parameters.AddWithValue("limit", limit);
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                return await ReadMessagesAsync<T>(reader).ConfigureAwait(false);
            }
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message) where T : class
    {
        return await SendDelayAsync(queueName, message, 0).ConfigureAwait(false);
    }

    public async Task<long> SendDelayAsync<T>(string queueName, T message, int delay) where T : class
    {
        var cmd = new NpgsqlCommand("SELECT * FROM pgmq.send(@queue_name, @message, @delay);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            cmd.Parameters.AddWithValue("message", NpgsqlDbType.Jsonb, SerializeMessage(message));
            cmd.Parameters.AddWithValue("delay", delay);
            var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return (long)result!;
        }
    }
    
    public async Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages) where T : class
    {
        var cmd = new NpgsqlCommand("SELECT * FROM pgmq.send_batch(@queue_name, @messages);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            cmd.Parameters.AddWithValue("messages", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
            var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                var result = new List<long>();
                while (await reader.ReadAsync().ConfigureAwait(false)) result.Add(reader.GetInt64(0));

                return result;
            }
        }
    }

    public async Task SetVtAsync(string queueName, long msgId, int vtOffset)
    {
        var cmd = new NpgsqlCommand("SELECT pgmq.set_vt(@queue_name, @msg_id, @vt_offset);", _connection);
        await using (cmd.ConfigureAwait(false))
        {
            cmd.Parameters.AddWithValue("queue_name", queueName);
            cmd.Parameters.AddWithValue("msg_id", msgId);
            cmd.Parameters.AddWithValue("vt_offset", vtOffset);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    private async Task<List<NpgmqMessage<T>>> ReadMessagesAsync<T>(DbDataReader reader) where T : class
    {
        var msgIdOrdinal = reader.GetOrdinal("msg_id");
        var readCtOrdinal = reader.GetOrdinal("read_ct");
        var enqueuedAtOrdinal = reader.GetOrdinal("enqueued_at");
        var vtOrdinal = reader.GetOrdinal("vt");
        var messageOrdinal = reader.GetOrdinal("message");

        var result = new List<NpgmqMessage<T>>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            result.Add(new NpgmqMessage<T>
            {
                MsgId = reader.GetInt64(msgIdOrdinal),
                ReadCt = reader.GetInt32(readCtOrdinal),
                EnqueuedAt = reader.GetDateTime(enqueuedAtOrdinal),
                Vt = reader.GetDateTime(vtOrdinal),
                Message = DeserializeMessage<T>(reader.GetString(messageOrdinal))
            });
        }
        return result;
    }

    private string SerializeMessage<T>(T message) where T : class
    {
        if (typeof(T) == typeof(string))
        {
            return message as string ?? "";
        }

        return JsonSerializer.Serialize(message);
    }

    private T? DeserializeMessage<T>(string message) where T : class
    {
        if (typeof(T) == typeof(string))
        {
            return (T?)(object?)message;
        }
        
        return JsonSerializer.Deserialize<T?>(message);
    }
}