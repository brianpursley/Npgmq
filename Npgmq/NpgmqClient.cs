﻿using System.Data.Common;
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

    private readonly NpgmqCommandFactory _commandFactory;

    /// <summary>
    /// Create a new <see cref="NpgmqClient"/>.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public NpgmqClient(string connectionString)
    {
        _commandFactory = new NpgmqCommandFactory(connectionString);
    }

    /// <summary>
    /// Create a new <see cref="NpgmqClient"/>.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    public NpgmqClient(NpgsqlConnection connection)
    {
        _commandFactory = new NpgmqCommandFactory(connection);
    }

    public async Task<bool> ArchiveAsync(string queueName, long msgId)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.archive(@queue_name, @msg_id);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return Convert.ToBoolean(result);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to archive message {msgId} in queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> ArchiveBatchAsync(string queueName, IEnumerable<long> msgIds)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.archive(@queue_name, @msg_ids);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_ids", msgIds);
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to archive messages in queue {queueName}.", ex);
        }
    }

    public async Task CreateQueueAsync(string queueName)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.create(@queue_name);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to create queue {queueName}.", ex);
        }
    }

    public async Task CreateUnloggedQueueAsync(string queueName)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.create_unlogged(@queue_name);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to create unlogged queue {queueName}.", ex);
        }
    }

    public async Task<bool> DeleteAsync(string queueName, long msgId)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.delete(@queue_name, @msg_id);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return Convert.ToBoolean(result!);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to delete message {msgId} from queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> DeleteBatchAsync(string queueName, IEnumerable<long> msgIds)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.delete(@queue_name, @msg_ids);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_ids", msgIds.ToArray());
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to delete messages from queue {queueName}.", ex);
        }
    }

    public async Task DropQueueAsync(string queueName)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.drop_queue(@queue_name);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to drop queue {queueName}.", ex);
        }
    }

    public async Task InitAsync()
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException("Failed to initialize PGMQ extension.", ex);
        }
    }

    public async Task<List<NpgmqQueue>> ListQueuesAsync()
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues();").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<NpgmqQueue>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        result.Add(new NpgmqQueue
                        {
                            QueueName = reader.GetString(0),
                            CreatedAt = reader.GetDateTime(1),
                            IsPartitioned = reader.GetBoolean(2),
                            IsUnlogged = reader.GetBoolean(3)
                        });
                    }
                    return result;
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException("Failed to list queues.", ex);
        }
    }

    public async Task<NpgmqMessage<T>?> PollAsync<T>(string queueName, int vt = DefaultVt, int pollTimeoutSeconds = DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = DefaultPollIntervalMilliseconds) where T : class
    {
        try
        {
            var result = await PollBatchAsync<T>(queueName, vt, 1, pollTimeoutSeconds, pollIntervalMilliseconds).ConfigureAwait(false);
            return result.SingleOrDefault();
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to poll queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> PollBatchAsync<T>(string queueName, int vt = DefaultVt, int limit = DefaultReadBatchLimit, int pollTimeoutSeconds = DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = DefaultPollIntervalMilliseconds) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read_with_poll(@queue_name, @vt, @limit, @poll_timeout_s, @poll_interval_ms);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@vt", vt);
                cmd.Parameters.AddWithValue("@limit", limit);
                cmd.Parameters.AddWithValue("@poll_timeout_s", pollTimeoutSeconds);
                cmd.Parameters.AddWithValue("@poll_interval_ms", pollIntervalMilliseconds);
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to poll queue {queueName}.", ex);
        }
    }

    public async Task<NpgmqMessage<T>?> PopAsync<T>(string queueName) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.pop(@queue_name);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = await ReadMessagesAsync<T>(reader).ConfigureAwait(false);
                    return result.SingleOrDefault();
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to pop queue {queueName}.", ex);
        }
    }

    public async Task<long> PurgeQueueAsync(string queueName)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.purge_queue(@queue_name);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to purge queue {queueName}.", ex);
        }
    }

    public async Task<bool> QueueExistsAsync(string queueName)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT 1 WHERE EXISTS (SELECT * FROM pgmq.list_queues() WHERE queue_name = @queue_name);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return Convert.ToInt32(result ?? 0) == 1;
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to check if queue {queueName} exists.", ex);
        }
    }

    public async Task<NpgmqMessage<T>?> ReadAsync<T>(string queueName, int vt = DefaultVt) where T : class
    {
        try
        {
            var result = await ReadBatchAsync<T>(queueName, vt, 1).ConfigureAwait(false);
            return result.SingleOrDefault();
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> ReadBatchAsync<T>(string queueName, int vt = DefaultVt, int limit = DefaultReadBatchLimit) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read(@queue_name, @vt, @limit);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@vt", vt);
                cmd.Parameters.AddWithValue("@limit", limit);
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task<long> SendAsync<T>(string queueName, T message) where T : class
    {
        try
        {
            return await SendDelayAsync(queueName, message, 0).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<long> SendDelayAsync<T>(string queueName, T message, int delay) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send(@queue_name, @message, @delay);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@message", NpgsqlDbType.Jsonb, SerializeMessage(message));
                cmd.Parameters.AddWithValue("@delay", delay);
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return Convert.ToInt64(result!);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send message to queue {queueName}.", ex);
        }
    }

    public async Task<List<long>> SendBatchAsync<T>(string queueName, IEnumerable<T> messages) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT * FROM pgmq.send_batch(@queue_name, @messages);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@messages", NpgsqlDbType.Array | NpgsqlDbType.Jsonb, messages.Select(SerializeMessage).ToArray());
                var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var result = new List<long>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to send messages to queue {queueName}.", ex);
        }
    }

    public async Task SetVtAsync(string queueName, long msgId, int vtOffset)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.set_vt(@queue_name, @msg_id, @vt_offset);").ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@msg_id", msgId);
                cmd.Parameters.AddWithValue("@vt_offset", vtOffset);
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new NpgmqException($"Failed to set VT for message {msgId} in queue {queueName}.", ex);
        }
    }

    private static async Task<List<NpgmqMessage<T>>> ReadMessagesAsync<T>(DbDataReader reader) where T : class
    {
        var result = new List<NpgmqMessage<T>>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            result.Add(new NpgmqMessage<T>
            {
                MsgId = reader.GetInt64(0),
                ReadCt = reader.GetInt32(1),
                EnqueuedAt = reader.GetDateTime(2),
                Vt = reader.GetDateTime(3),
                Message = DeserializeMessage<T>(reader.GetString(4))
            });
        }
        return result;
    }

    private static string SerializeMessage<T>(T message) where T : class => 
        typeof(T) == typeof(string) ? message as string ?? "" : JsonSerializer.Serialize(message); 

    private static T? DeserializeMessage<T>(string message) where T : class =>
        typeof(T) == typeof(string) ? (T?)(object?)message : JsonSerializer.Deserialize<T?>(message);
}
