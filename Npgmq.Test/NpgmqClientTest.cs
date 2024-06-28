using System.Text.Json;
using Dapper;
using DeepEqual.Syntax;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Npgmq.Test;

public sealed class NpgmqClientTest : IDisposable
{
    private static readonly string TestQueueName = $"test_{Guid.NewGuid():N}";
    
    private readonly string _connectionString;
    private readonly NpgsqlConnection _connection;
    private readonly NpgmqClient _sut;

    private class TestMessage
    {
        public int? Foo { get; set; }
        public string? Bar { get; set; }
        public DateTimeOffset? Baz { get; set; }
    }

    public NpgmqClientTest()
    {
        var configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .AddUserSecrets<NpgmqClientTest>()
            .Build();

        _connectionString = configuration.GetConnectionString("Test")!;
        _connection = new NpgsqlConnection(_connectionString);
        _sut = new NpgmqClient(_connection);
    }
    
    public void Dispose()
    {
        _connection.Close();
        _connection.Dispose();
    }
    
    private async Task ResetTestQueueAsync()
    {
        if (await _sut.QueueExistsAsync(TestQueueName))
        {
            await _sut.DropQueueAsync(TestQueueName);
        }
        await _sut.CreateQueueAsync(TestQueueName);
    }

    [Fact]
    public async Task ArchiveAsync_should_archive_a_single_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var result = await _sut.ArchiveAsync(TestQueueName, msgId);

        // Assert
        Assert.True(result);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{TestQueueName};"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.a_{TestQueueName} LIMIT 1;"));
    }
    
    [Fact]
    public async Task ArchiveAsync_should_return_false_if_message_not_found()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var result = await _sut.ArchiveAsync(TestQueueName, 1);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ArchiveBatchAsync_should_archive_multiple_messages()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 3 })
        };

        var results = await _sut.ArchiveBatchAsync(TestQueueName, msgIds);

        // Assert
        Assert.Equal(msgIds, results);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{TestQueueName};"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.a_{TestQueueName} ORDER BY msg_id;")).ToList());
    }
    
    [Fact]
    public async Task CreateQueueAsync_should_create_a_queue()
    {
        // Arrange
        if (await _sut.QueueExistsAsync(TestQueueName))
        {
            await _sut.DropQueueAsync(TestQueueName);
        }
        
        // Act
        await _sut.CreateQueueAsync(TestQueueName);
        
        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName and is_partitioned = false and is_unlogged = false;", new { queueName = TestQueueName }));
    }
    
    [Fact]
    public async Task CreateUnloggedQueueAsync_should_create_an_unlogged_queue()
    {
        // Arrange
        if (await _sut.QueueExistsAsync(TestQueueName))
        {
            await _sut.DropQueueAsync(TestQueueName);
        }
        
        // Act
        await _sut.CreateUnloggedQueueAsync(TestQueueName);
        
        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName and is_partitioned = false and is_unlogged = true;", new { queueName = TestQueueName }));
    }

    [Fact]
    public async Task DeleteAsync_should_delete_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var result = await _sut.DeleteAsync(TestQueueName, msgId);

        // Assert
        Assert.True(result);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{TestQueueName};"));
    }
    
    [Fact]
    public async Task DeleteAsync_should_return_false_if_message_not_found()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var result = await _sut.DeleteAsync(TestQueueName, 1);

        // Assert
        Assert.False(result);
    }
    
    [Fact]
    public async Task DeleteBatchAsync_should_delete_multiple_messages()
    {
        // Arrange
        await ResetTestQueueAsync();
        var msgIds = new List<long>
        {
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 3 })
        };
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        
        // Act
        var results = await _sut.DeleteBatchAsync(TestQueueName, msgIds);

        // Assert
        Assert.Equal(msgIds, results);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{TestQueueName};"));
    }

    [Fact]
    public async Task DropQueueAsync_should_drop_queue()
    {
        // Arrange
        await ResetTestQueueAsync();
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName;", new { queueName = TestQueueName }));
        
        // Act
        await _sut.DropQueueAsync(TestQueueName);

        // Assert
        Assert.Equal(0, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName;", new { queueName = TestQueueName }));
    }

    [Fact]
    public async Task InitAsync_should_initialize_pgmq_extension()
    {
        // Arrange
        await _connection.ExecuteAsync("DROP EXTENSION IF EXISTS pgmq CASCADE;");
        Assert.Equal(0, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pg_extension WHERE extname = 'pgmq';"));

        // Act
        await _sut.InitAsync();
        
        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pg_extension WHERE extname = 'pgmq';"));
        
        // Act (Calling it again should not throw an exception)
        await _sut.InitAsync();
        
        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pg_extension WHERE extname = 'pgmq';"));
    }

    [Fact]
    public async Task ListQueuesAsync_should_return_list_of_queues()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var queues = await _sut.ListQueuesAsync();

        // Assert
        var queue = Assert.Single(queues);
        Assert.Equal(TestQueueName, queue.QueueName);
        Assert.False(queue.IsPartitioned);
        Assert.False(queue.IsUnlogged);
    }
    
    [Fact]
    public async Task PollAsync_should_wait_for_message_and_return_it()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var pollTask = _sut.PollAsync<TestMessage>(TestQueueName);
        await Task.Delay(1000);

        var producer = new NpgmqClient(_connectionString);
        await producer.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });
        
        var msg = await pollTask;
        
        // Assert
        Assert.NotNull(msg);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.Equal(1, msg.ReadCt);
        msg.Message.ShouldDeepEqual(new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });
    }

    [Fact]
    public async Task PollAsync_should_return_null_if_timeout_occurs_before_a_message_is_available_tp_read()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var pollTask = _sut.PollAsync<TestMessage>(TestQueueName, pollTimeoutSeconds: 1);
        await Task.Delay(1100);
        await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });
        var msg = await pollTask;
        
        // Assert
        Assert.Null(msg);
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
    }

    [Fact]
    public async Task PollBatchAsync_should_poll_for_multiple_messages()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var pollTask = _sut.PollBatchAsync<TestMessage>(TestQueueName, limit: 3);

        // Wait a little bit and then send some messages
        await Task.Delay(1000);
        var producer = new NpgmqClient(_connectionString);
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 1 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 2 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 3 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 4 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 5 });
        
        // Get the messages received by the poll
        var messages = await pollTask;
        
        // Assert
        Assert.True(messages.Any());
        Assert.True(messages.Count <= 3);
        // TODO: Improve this test, keeping in mind that each call to PollBatchAsync is not guaranteed to read the limit
    }

    [Fact]
    public async Task PollBatchAsync_should_poll_for_multiple_messages_in_multiple_batches()
    {
        // Arrange
        await ResetTestQueueAsync();

        // Act
        var pollTask = _sut.PollBatchAsync<TestMessage>(TestQueueName, limit: 3);

        // Wait a little bit and then send some messages
        await Task.Delay(1000);
        var producer = new NpgmqClient(_connectionString);
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 1 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 2 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 3 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 4 });
        await producer.SendAsync(TestQueueName, new TestMessage { Foo = 5 });

        // Get the messages received by the poll
        var batch1 = await pollTask;

        // Poll again to receive the other messages.
        var batch2 = await _sut.PollBatchAsync<TestMessage>(TestQueueName, limit: 3, pollTimeoutSeconds: 1);

        // Assert
        Assert.True(batch1.Any());
        Assert.True(batch1.Count <= 3);
        Assert.True(batch2.Any());
        Assert.True(batch2.Count <= 3);
        Assert.Equal(batch1.Count + batch2.Count, batch1.Select(x => x.MsgId).Union(batch2.Select(x => x.MsgId)).Count());
    }
    
    [Fact]
    public async Task PopAsync_should_read_and_delete_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.PopAsync<TestMessage>(TestQueueName);

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(msgId, msg.MsgId);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.Equal(0, msg.ReadCt);
        msg.Message.ShouldDeepEqual(new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{TestQueueName};"));
    }

    [Fact]
    public async Task PopAsync_should_return_null_if_no_message_is_available()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msg = await _sut.PopAsync<TestMessage>(TestQueueName);

        // Assert
        Assert.Null(msg);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
    }

    [Fact]
    public async Task PurgeQueueAsync_should_delete_all_messages_from_a_queue()
    {
        // Arrange
        await ResetTestQueueAsync();
        await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 1 });
        await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 2 });
        await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 3 });
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        
        // Act
        var purgeCount = await _sut.PurgeQueueAsync(TestQueueName);

        // Assert
        Assert.Equal(3, purgeCount);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
    }

    [Fact]
    public async Task QueueExistsAsync_should_return_true_if_queue_exists()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var result = await _sut.QueueExistsAsync(TestQueueName);

        // Assert
        Assert.True(result);
    }
    
    [Fact]
    public async Task QueueExistsAsync_should_return_false_if_queue_does_not_exist()
    {
        // Arrange
        await ResetTestQueueAsync();
        await _sut.DropQueueAsync(TestQueueName);
        
        // Act
        var result = await _sut.QueueExistsAsync(TestQueueName);

        // Assert
        Assert.False(result);
    }
    
    [Fact]
    public async Task ReadAsync_should_read_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.ReadAsync<TestMessage>(TestQueueName);

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(msgId, msg.MsgId);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.True(msg.Vt > DateTimeOffset.UtcNow);
        Assert.Equal(1, msg.ReadCt);
        msg.Message.ShouldDeepEqual(new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });
    }

    [Fact]
    public async Task ReadAsync_should_read_string_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.ReadAsync<string>(TestQueueName);

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(msgId, msg.MsgId);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.True(msg.Vt > DateTimeOffset.UtcNow);
        Assert.Equal(1, msg.ReadCt);
        msg.Message.ShouldDeepEqual("{\"Bar\": \"Test\", \"Baz\": \"2023-09-01T01:23:45-04:00\", \"Foo\": 123}");
    }

    [Fact]
    public async Task ReadAsync_should_return_null_if_no_message_is_available()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msg = await _sut.ReadAsync<TestMessage>(TestQueueName);

        // Assert
        Assert.Null(msg);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
    }

    [Fact]
    public async Task ReadBatchAsync_should_return_list_of_messages()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 3 })
        };

        var messages = await _sut.ReadBatchAsync<TestMessage>(TestQueueName);

        // Assert
        messages.Select(x => x.MsgId).OrderBy(x => x).ShouldDeepEqual(msgIds.OrderBy(x => x));
        messages.Select(x => x.Message).OrderBy(x => x!.Foo).ShouldDeepEqual(new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        });
    }

    [Fact]
    public async Task ConnectionString_should_be_used_to_connect()
    {
        // Arrange
        await ResetTestQueueAsync();
        var sut2 = new NpgmqClient(_connectionString);

        // Act
        var msgId = await sut2.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_add_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_commit_with_database_transaction()
    {
        // Arrange
        await ResetTestQueueAsync();
        await using var connection2 = new NpgsqlConnection(_connectionString);
        await connection2.OpenAsync();

        // Act
        await using var transaction = await _connection.BeginTransactionAsync();
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));

        // Act
        await transaction.CommitAsync();

        // Assert
        Assert.Equal(1, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(1, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await connection2.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_rollback_with_database_transaction()
    {
        // Arrange
        await ResetTestQueueAsync();
        await using var connection2 = new NpgsqlConnection(_connectionString);
        await connection2.OpenAsync();

        // Act
        await using var transaction = await _connection.BeginTransactionAsync();
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));

        // Act
        await transaction.RollbackAsync();

        // Assert
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(0, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
    }

    [Fact]
    public async Task SendAsync_should_add_string_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var message = "{\"Foo\": 123, \"Bar\": \"Test\", \"Baz\": \"2023-09-01T01:23:45-04:00\"}";
        var msgId = await _sut.SendAsync(TestQueueName, message); 
        
        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));
        var actualMessage = await _connection.ExecuteScalarAsync<string>($"SELECT message FROM pgmq.q_{TestQueueName} LIMIT 1;");
        JsonDocument.Parse(actualMessage!).ShouldDeepEqual(JsonDocument.Parse(message));
    }

    [Fact]
    public async Task SendDelayAsync_should_add_message_with_future_vt()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendDelayAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, 100);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt > CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgIds = await _sut.SendBatchAsync(TestQueueName, new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        });

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task SetVtAsync_should_change_vt_for_a_message()
    {
        // Arrange
        await ResetTestQueueAsync();
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 1 });
        var message1 = await _sut.ReadAsync<TestMessage>(TestQueueName);
        Assert.NotNull(message1);
        Assert.Equal(msgId, message1.MsgId);
        Assert.Null(await _sut.ReadAsync<TestMessage>(TestQueueName));
        
        // Act
        await _sut.SetVtAsync(TestQueueName, msgId, -60);
        var message2 = await _sut.ReadAsync<TestMessage>(TestQueueName);
        
        // Assert
        Assert.NotNull(message2);
        Assert.Equal(msgId, message2.MsgId);
    }
}
