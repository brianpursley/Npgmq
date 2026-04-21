namespace Npgmq;

public partial class NpgmqClient
{
    public async Task<List<NpgmqMessage<T>>> PollGroupedAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, int pollTimeoutSeconds = INpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = INpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped_with_poll(@queue_name, @vt, @qty, @max_poll_seconds, @poll_interval_ms);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@vt", vt);
                cmd.Parameters.AddWithValue("@qty", limit);
                cmd.Parameters.AddWithValue("@max_poll_seconds", pollTimeoutSeconds);
                cmd.Parameters.AddWithValue("@poll_interval_ms", pollIntervalMilliseconds);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to poll queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> PollGroupedRoundRobinAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, int pollTimeoutSeconds = INpgmqClient.DefaultPollTimeoutSeconds, int pollIntervalMilliseconds = INpgmqClient.DefaultPollIntervalMilliseconds, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped_rr_with_poll(@queue_name, @vt, @qty, @max_poll_seconds, @poll_interval_ms);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                cmd.Parameters.AddWithValue("@vt", vt);
                cmd.Parameters.AddWithValue("@qty", limit);
                cmd.Parameters.AddWithValue("@max_poll_seconds", pollTimeoutSeconds);
                cmd.Parameters.AddWithValue("@poll_interval_ms", pollIntervalMilliseconds);
                var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    return await ReadMessagesAsync<T>(reader, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to poll queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> ReadGroupedAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped(@queue_name, @vt, @qty);", cancellationToken).ConfigureAwait(false);
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> ReadGroupedRoundRobinAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped_rr(@queue_name, @vt, @qty);", cancellationToken).ConfigureAwait(false);
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task<List<NpgmqMessage<T>>> ReadGroupedHeadAsync<T>(string queueName, int vt = INpgmqClient.DefaultVt, int limit = INpgmqClient.DefaultReadBatchLimit, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped_head(@queue_name, @vt, @qty);", cancellationToken).ConfigureAwait(false);
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to read from queue {queueName}.", ex);
        }
    }

    public async Task CreateFifoIndexAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.create_fifo_index(@queue_name);", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                cmd.Parameters.AddWithValue("@queue_name", queueName);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException($"Failed to create FIFO index for queue {queueName}.", ex);
        }
    }

    public async Task CreateFifoIndexesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var cmd = await _commandFactory.CreateAsync("SELECT pgmq.create_fifo_indexes_all();", cancellationToken).ConfigureAwait(false);
            await using (cmd.ConfigureAwait(false))
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new NpgmqException("Failed to create all FIFO indexes.", ex);
        }
    }
}