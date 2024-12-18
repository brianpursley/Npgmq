using System.Diagnostics.CodeAnalysis;
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

    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
    private class TestMessage
    {
        public int? Foo { get; init; }
        public string? Bar { get; init; }
        public DateTimeOffset? Baz { get; init; }
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

    private async Task<bool> IsMinPgmqVersion(string minVersion)
    {
        var version = await _connection.ExecuteScalarAsync<string>("select extversion from pg_extension where extname = 'pgmq';");
        return version is not null && new Version(version) >= new Version(minVersion);
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
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
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
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
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
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
        Assert.True(msg.Vt > DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.Vt.Offset);
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
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
        Assert.True(msg.Vt > DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.Vt.Offset);
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
    public async Task SendAsync_with_int_delay_should_add_message_with_future_vt()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
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

    [SkippableFact]
    public async Task SendAsync_with_timestamp_delay_should_add_message_with_a_specified_vt()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.5.0"), "This test requires pgmq 1.5.0 or later");
        
        // Arrange
        await ResetTestQueueAsync();
        var delay = DateTimeOffset.UtcNow.AddHours(1);

        // Act
        var msgId = await _sut.SendAsync(TestQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, delay);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt = @expected_vt;", new { expected_vt = delay }));
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
    public async Task SendBatchAsync_with_int_delay_should_add_multiple_messages_with_future_vt()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        // Act
        var msgIds = await _sut.SendBatchAsync(TestQueueName, new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        }, 100);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt > CURRENT_TIMESTAMP;"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{TestQueueName} ORDER BY msg_id;")).ToList());
    }

    [SkippableFact]
    public async Task SendBatchAsync_with_timestamp_delay_should_add_multiple_messages_with_a_specified_vt()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.5.0"), "This test requires pgmq 1.5.0 or later");

        // Arrange
        await ResetTestQueueAsync();
        var delay = DateTimeOffset.UtcNow.AddHours(1);
        
        // Act
        var msgIds = await _sut.SendBatchAsync(TestQueueName, new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        }, delay);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{TestQueueName} WHERE vt = @expected_vt;", new { expected_vt = delay }));
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

    [Fact]
    public async Task GetMetricsAsync_should_return_metrics_for_a_single_queue()
    {
        // Arrange
        await ResetTestQueueAsync();
        
        var metrics1 = await _sut.GetMetricsAsync(TestQueueName);
        await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 1 });
        await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 2 });
        var msgId3 = await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 3 });
        var msgId4 = await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 4 });
        await _sut.SendAsync(TestQueueName, new TestMessage { Foo = 5 });
        await _sut.DeleteAsync(TestQueueName, msgId3);        
        await _sut.ArchiveAsync(TestQueueName, msgId4);

        // Act
        var metrics2 = await _sut.GetMetricsAsync(TestQueueName);
        await _sut.ReadAsync<string>(TestQueueName);
        var metrics3 = await _sut.GetMetricsAsync(TestQueueName);
        await _sut.PurgeQueueAsync(TestQueueName);
        var metrics4 = await _sut.GetMetricsAsync(TestQueueName);
        
        // Assert
        Assert.Equal(TestQueueName, metrics1.QueueName);
        Assert.Equal(0, metrics1.QueueLength);
        Assert.Null(metrics1.NewestMessageAge);
        Assert.Null(metrics1.OldestMessageAge);
        if (await IsMinPgmqVersion("0.33.1"))
        {
            // There was a bug in PGMQ prior to 0.33.1 that caused the total messages to be incorrect,
            // so we only want to check the result if the version is 0.33.1 or later.
            Assert.Equal(0, metrics1.TotalMessages);
        }
        Assert.True(metrics1.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);
        if (await IsMinPgmqVersion("1.5.0"))
        {
            Assert.Equal(0, metrics1.QueueVisibleLength);
        }
        else
        {
            // The QueueVisibleLength metric was added in PGMQ 1.5.0, so it will be -1 if the version is older.
            Assert.Equal(-1, metrics1.QueueVisibleLength);
        }

        Assert.Equal(TestQueueName, metrics2.QueueName);
        Assert.Equal(3, metrics2.QueueLength);
        Assert.True(metrics2.NewestMessageAge >= 0);
        Assert.True(metrics2.OldestMessageAge >= 0);
        if (await IsMinPgmqVersion("0.33.1"))
        {
            // There was a bug in PGMQ prior to 0.33.1 that caused the total messages to be incorrect,
            // so we only want to check the result if the version is 0.33.1 or later.
            Assert.Equal(5, metrics2.TotalMessages);
        }
        Assert.True(metrics2.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);
        if (await IsMinPgmqVersion("1.5.0"))
        {
            Assert.Equal(3, metrics2.QueueVisibleLength);
        }
        else
        {
            // The QueueVisibleLength metric was added in PGMQ 1.5.0, so it will be -1 if the version is older.
            Assert.Equal(-1, metrics2.QueueVisibleLength);
        }

        Assert.Equal(TestQueueName, metrics3.QueueName);
        Assert.Equal(3, metrics3.QueueLength);
        Assert.True(metrics3.NewestMessageAge >= 0);
        Assert.True(metrics3.OldestMessageAge >= 0);
        if (await IsMinPgmqVersion("0.33.1"))
        {
            // There was a bug in PGMQ prior to 0.33.1 that caused the total messages to be incorrect,
            // so we only want to check the result if the version is 0.33.1 or later.
            Assert.Equal(5, metrics3.TotalMessages);
        }
        Assert.True(metrics3.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);
        if (await IsMinPgmqVersion("1.5.0"))
        {
            Assert.Equal(2, metrics3.QueueVisibleLength);
        }
        else
        {
            // The QueueVisibleLength metric was added in PGMQ 1.5.0, so it will be -1 if the version is older.
            Assert.Equal(-1, metrics3.QueueVisibleLength);
        }
        
        Assert.Equal(TestQueueName, metrics4.QueueName);
        Assert.Equal(0, metrics4.QueueLength);
        Assert.Null(metrics1.NewestMessageAge);
        Assert.Null(metrics1.OldestMessageAge);
        if (await IsMinPgmqVersion("0.33.1"))
        {
            // There was a bug in PGMQ prior to 0.33.1 that caused the total messages to be incorrect,
            // so we only want to check the result if the version is 0.33.1 or later.
            Assert.Equal(5, metrics4.TotalMessages);
        }
        Assert.True(metrics4.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);
        if (await IsMinPgmqVersion("1.5.0"))
        {
            Assert.Equal(0, metrics4.QueueVisibleLength);
        }
        else
        {
            // The QueueVisibleLength metric was added in PGMQ 1.5.0, so it will be -1 if the version is older.
            Assert.Equal(-1, metrics4.QueueVisibleLength);
        }
    }
    
    [Fact]
    public async Task GetMetricsAsync_should_return_metrics_for_all_queues()
    {
        // Create some queues just for testing this function.
        var testMetricsQueueName1 = TestQueueName + "_m1";
        var testMetricsQueueName2 = TestQueueName + "_m2";
        var testMetricsQueueName3 = TestQueueName + "_m3";
        try
        {
            // Arrange
            await ResetTestQueueAsync();
            await _sut.CreateQueueAsync(testMetricsQueueName1);
            await _sut.CreateQueueAsync(testMetricsQueueName2);
            await _sut.CreateQueueAsync(testMetricsQueueName3);

            await _sut.SendAsync(testMetricsQueueName1, new TestMessage { Foo = 1 });
            await _sut.SendAsync(testMetricsQueueName1, new TestMessage { Foo = 2 });
            await _sut.SendAsync(testMetricsQueueName1, new TestMessage { Foo = 3 });
            await _sut.SendAsync(testMetricsQueueName2, new TestMessage { Foo = 4 });
            await _sut.SendAsync(testMetricsQueueName2, new TestMessage { Foo = 5 });

            // Act
            var allMetrics = await _sut.GetMetricsAsync();

            // Assert
            Assert.Equal(3, allMetrics.Single(x => x.QueueName == testMetricsQueueName1).QueueLength);
            Assert.Equal(2, allMetrics.Single(x => x.QueueName == testMetricsQueueName2).QueueLength);
            Assert.Equal(0, allMetrics.Single(x => x.QueueName == testMetricsQueueName3).QueueLength);
        }
        finally
        {
            try { await _sut.DropQueueAsync(testMetricsQueueName1); } catch { /* ignored */ }
            try { await _sut.DropQueueAsync(testMetricsQueueName2); } catch { /* ignored */ }
            try { await _sut.DropQueueAsync(testMetricsQueueName3); } catch { /* ignored */ }
        }
    }
}
