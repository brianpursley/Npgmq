using System.Data;
using System.Text.Json;
using Dapper;
using DeepEqual.Syntax;
using JOS.SystemTextJson.DictionaryStringObject.JsonConverter;
using Npgsql;

namespace Npgmq.Test;

[Collection("Npgmq")]
public sealed partial class NpgmqClientTest : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _postgresFixture;
    private readonly NpgsqlConnection _connection;
    private readonly NpgmqClient _sut;
    private readonly string _testQueueName = $"test_{Guid.NewGuid():N}";

    private static readonly JsonSerializerOptions DeserializationOptions = new()
    {
        Converters = { new DictionaryStringObjectJsonConverter() }
    };

    private class TestMessage
    {
        public int? Foo { get; init; }
        public string? Bar { get; init; }
        public DateTimeOffset? Baz { get; init; }
    }

    public NpgmqClientTest(PostgresFixture postgresFixture)
    {
        _postgresFixture = postgresFixture;
        _sut = new NpgmqClient(_postgresFixture.DataSource);
        _connection = _postgresFixture.DataSource.OpenConnection();
    }

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        if (await _sut.QueueExistsAsync(_testQueueName))
        {
            await _sut.DropQueueAsync(_testQueueName);
        }
        await _connection.CloseAsync();
        await _connection.DisposeAsync();
    }

    private async Task<bool> IsMinPgmqVersion(string minVersion)
    {
        var version = await _connection.ExecuteScalarAsync<string>("select extversion from pg_extension where extname = 'pgmq';");
        return version is not null && new Version(version) >= new Version(minVersion);
    }

    [Fact]
    public void NpgmqClient_should_throw_exception_when_provided_connection_is_null()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new NpgmqClient((NpgsqlConnection)null!));
        Assert.Equal("connection", ex.ParamName);
    }

    [Fact]
    public void NpgmqClient_should_throw_exception_when_provided_data_source_is_null()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new NpgmqClient((NpgsqlDataSource)null!));
        Assert.Equal("dataSource", ex.ParamName);
    }

    [Fact]
    public void NpgmqClient_should_throw_exception_when_provided_connection_string_is_null()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new NpgmqClient((string)null!));
        Assert.Equal("connectionString", ex.ParamName);
    }

    [Fact]
    public async Task NpgmqClient_should_work_when_created_using_a_connection_string()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        var sut = new NpgmqClient(_postgresFixture.ConnectionString); // Don't use _sut here, as we want to test a new instance

        // Act
        await sut.SendAsync(_testQueueName, new TestMessage { Foo = 123 });
        var msg = await sut.ReadAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.NotNull(msg);
        msg.Message.ShouldDeepEqual(new TestMessage { Foo = 123 });

        // Cleanup
        NpgsqlConnection.ClearAllPools();
    }

    [Fact]
    public async Task NpgmqClient_should_work_when_created_using_an_opened_connection()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await using var connection = await _postgresFixture.DataSource.OpenConnectionAsync();
        var sut = new NpgmqClient(connection); // Don't use _sut here, as we want to test a new instance

        // Act
        await sut.SendAsync(_testQueueName, new TestMessage { Foo = 123 });
        var msg = await sut.ReadAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.NotNull(msg);
        msg.Message.ShouldDeepEqual(new TestMessage { Foo = 123 });
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task NpgmqClient_should_work_when_created_using_an_unopened_connection()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await using var connection = _postgresFixture.DataSource.CreateConnection(); // Create connection, but don't open it
        var sut = new NpgmqClient(connection); // Don't use _sut here, as we want to test a new instance

        // Act
        await sut.SendAsync(_testQueueName, new TestMessage { Foo = 123 });
        var msg = await sut.ReadAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.NotNull(msg);
        msg.Message.ShouldDeepEqual(new TestMessage { Foo = 123 });
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task ArchiveAsync_should_archive_a_single_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var result = await _sut.ArchiveAsync(_testQueueName, msgId);

        // Assert
        Assert.True(result);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.a_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task ArchiveAsync_should_return_false_if_message_not_found()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var result = await _sut.ArchiveAsync(_testQueueName, 1);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ArchiveBatchAsync_should_archive_multiple_messages()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 })
        };

        var results = await _sut.ArchiveBatchAsync(_testQueueName, msgIds);

        // Assert
        Assert.Equal(msgIds, results);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.a_{_testQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task CreateQueueAsync_should_create_a_queue()
    {
        // Arrange

        // Act
        await _sut.CreateQueueAsync(_testQueueName);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName and is_partitioned = false and is_unlogged = false;", new { queueName = _testQueueName }));
    }

    [Fact]
    public async Task CreateUnloggedQueueAsync_should_create_an_unlogged_queue()
    {
        // Arrange

        // Act
        await _sut.CreateUnloggedQueueAsync(_testQueueName);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName and is_partitioned = false and is_unlogged = true;", new { queueName = _testQueueName }));
    }

    [Fact]
    public async Task DeleteAsync_should_delete_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var result = await _sut.DeleteAsync(_testQueueName, msgId);

        // Assert
        Assert.True(result);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
    }

    [Fact]
    public async Task DeleteAsync_should_return_false_if_message_not_found()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var result = await _sut.DeleteAsync(_testQueueName, 1);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task DeleteBatchAsync_should_delete_multiple_messages()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 })
        };
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));

        // Act
        var results = await _sut.DeleteBatchAsync(_testQueueName, msgIds);

        // Assert
        Assert.Equal(msgIds, results);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
    }

    [Fact]
    public async Task DropQueueAsync_should_drop_queue()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        Assert.Equal(1, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName;", new { queueName = _testQueueName }));

        // Act
        var result = await _sut.DropQueueAsync(_testQueueName);

        // Assert
        Assert.True(result);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pgmq.meta WHERE queue_name = @queueName;", new { queueName = _testQueueName }));
    }

    [Fact]
    public async Task DropQueueAsync_should_return_false_if_queue_does_not_exist()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var result = await _sut.DropQueueAsync("some_nonexistent_queue");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task InitAsync_should_initialize_pgmq_extension()
    {
        try
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
        finally
        {
            // Cleanup
            await _sut.InitAsync();
        }
    }

    [Fact]
    public async Task GetPgmqVersionAsync_should_return_pgmq_version()
    {
        // Arrange
        var expectedVersionString = await _connection.ExecuteScalarAsync<string>("SELECT extversion FROM pg_extension WHERE extname = 'pgmq';");
        var expectedVersion = new Version(expectedVersionString!);

        // Act
        var version = await _sut.GetPgmqVersionAsync();

        // Assert
        Assert.Equal(expectedVersion, version);
    }

    [Fact]
    public async Task GetPgmqVersionAsync_should_return_null_if_pgmq_is_not_installed()
    {
        try
        {
            // Arrange
            await _sut.CreateQueueAsync(_testQueueName);
            await _connection.ExecuteAsync("DROP EXTENSION IF EXISTS pgmq CASCADE;");
            Assert.Equal(0, await _connection.ExecuteScalarAsync<int>("SELECT count(*) FROM pg_extension WHERE extname = 'pgmq';"));

            // Act
            var version = await _sut.GetPgmqVersionAsync();

            // Assert
            Assert.Null(version);
        }
        finally
        {
            // Cleanup
            await _sut.InitAsync();
        }
    }

    [Fact]
    public async Task ListQueuesAsync_should_return_list_of_queues()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var queues = await _sut.ListQueuesAsync();

        // Assert
        var queue = Assert.Single(queues);
        Assert.Equal(_testQueueName, queue.QueueName);
        Assert.True(queue.CreatedAt < DateTimeOffset.UtcNow);
        Assert.False(queue.IsPartitioned);
        Assert.False(queue.IsUnlogged);
    }

    [Fact]
    public async Task PollAsync_should_wait_for_message_and_return_it()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var pollTask = _sut.PollAsync<TestMessage>(_testQueueName);
        await Task.Delay(1000);

        var producer = new NpgmqClient(_postgresFixture.DataSource);
        await producer.SendAsync(_testQueueName, new TestMessage
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
        Assert.Null(msg.Headers);
    }

    [Fact]
    public async Task PollAsync_should_return_null_if_timeout_occurs_before_a_message_is_available_tp_read()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var pollTask = _sut.PollAsync<TestMessage>(_testQueueName, pollTimeoutSeconds: 1);
        await Task.Delay(1100);
        await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });
        var msg = await pollTask;

        // Assert
        Assert.Null(msg);
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
    }

    [Fact]
    public async Task PollBatchAsync_should_poll_for_multiple_messages()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var pollTask = _sut.PollBatchAsync<TestMessage>(_testQueueName, limit: 3);

        // Wait a little bit and then send some messages
        await Task.Delay(1000);
        var producer = new NpgmqClient(_postgresFixture.DataSource);
        var msgIds = await producer.SendBatchAsync(
            _testQueueName,
            [
                new TestMessage { Foo = 1 },
                new TestMessage { Foo = 2 },
                new TestMessage { Foo = 3 },
                new TestMessage { Foo = 4 },
                new TestMessage { Foo = 5 }
            ]
        );

        // Get the messages received by the poll
        var messages = await pollTask;

        // Assert
        messages.Select(m => m.MsgId).ShouldDeepEqual(new[] { msgIds[0], msgIds[1], msgIds[2] });
    }

    [Fact]
    public async Task PollBatchAsync_should_poll_for_multiple_messages_in_multiple_batches()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var pollTask = _sut.PollBatchAsync<TestMessage>(_testQueueName, limit: 3);

        // Wait a little bit and then send some messages
        await Task.Delay(1000);
        var producer = new NpgmqClient(_postgresFixture.DataSource);
        var msgIds = await producer.SendBatchAsync(
            _testQueueName,
            [
                new TestMessage { Foo = 1 },
                new TestMessage { Foo = 2 },
                new TestMessage { Foo = 3 },
                new TestMessage { Foo = 4 },
                new TestMessage { Foo = 5 }
            ]
        );

        // Get the messages received by the poll
        var batch1 = await pollTask;

        // Poll again to receive the other messages.
        var batch2 = await _sut.PollBatchAsync<TestMessage>(_testQueueName, limit: 3, pollTimeoutSeconds: 1);

        // Assert
        Assert.True(batch1.Count > 0);
        Assert.True(batch1.Count <= 3);
        Assert.True(batch2.Count > 0);
        Assert.True(batch2.Count <= 3);
        var combined = batch1.Select(x => x.MsgId).Union(batch2.Select(x => x.MsgId)).ToList();
        combined.ShouldDeepEqual(new[] { msgIds[0], msgIds[1], msgIds[2], msgIds[3], msgIds[4] });
    }

    [Fact]
    public async Task PopAsync_should_read_and_delete_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.PopAsync<TestMessage>(_testQueueName);

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
        Assert.Null(msg.Headers);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
    }

    [Fact]
    public async Task PopAsync_should_read_and_delete_message_with_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var headers = new Dictionary<string, object>()
        {
            { "CorrelationId", "abc-123" },
            { "Priority", 5 }
        };
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, headers);

        var msg = await _sut.PopAsync<TestMessage>(_testQueueName);

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
        msg.Headers.ShouldDeepEqual(headers);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
    }

    [Fact]
    public async Task PopAsync_should_read_and_delete_string_object_dictionary_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.PopAsync<Dictionary<string, object>>(_testQueueName);

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(msgId, msg.MsgId);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
        Assert.Equal(0, msg.ReadCt);
        msg.Message.ShouldDeepEqual(new Dictionary<string, object>
        {
            { "Foo", 123 },
            { "Bar", "Test" },
            { "Baz", DateTimeOffset.Parse("2023-09-01T01:23:45-04:00") }
        });
        Assert.Null(msg.Headers);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
    }

    [SkippableTheory]
    [InlineData(1, 1, 2)]
    [InlineData(2, 2, 1)]
    [InlineData(3, 3, 0)]
    [InlineData(4, 3, 0)]
    public async Task PopAsync_should_read_and_delete_multiple_messages(int qty, int expectedCount, int expectedRemaining)
    {
        Skip.IfNot(await IsMinPgmqVersion("1.7.0"), "requires pgmq 1.7.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 })
        };

        var results = await _sut.PopAsync<TestMessage>(_testQueueName, qty);

        // Assert
        Assert.Equal(expectedCount, results.Count);
        for (var i = 0; i < expectedCount; i++)
        {
            var r = results[i];
            Assert.Equal(msgIds[i], r.MsgId);
            Assert.True(r.EnqueuedAt < DateTimeOffset.UtcNow);
            Assert.Equal(TimeSpan.Zero, r.EnqueuedAt.Offset);
            Assert.Equal(0, r.ReadCt);
            r.Message.ShouldDeepEqual(new TestMessage { Foo = i + 1 });
            Assert.Null(r.Headers);
        }
        Assert.Equal(expectedRemaining, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.a_{_testQueueName};"));
    }

    [Fact]
    public async Task PopAsync_should_return_null_if_no_message_is_available()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msg = await _sut.PopAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Null(msg);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
    }

    [Fact]
    public async Task PurgeQueueAsync_should_delete_all_messages_from_a_queue()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 });
        await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 });
        await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 });
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));

        // Act
        var purgeCount = await _sut.PurgeQueueAsync(_testQueueName);

        // Assert
        Assert.Equal(3, purgeCount);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
    }

    [Fact]
    public async Task QueueExistsAsync_should_return_true_if_queue_exists()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var result = await _sut.QueueExistsAsync(_testQueueName);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task QueueExistsAsync_should_return_false_if_queue_does_not_exist()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.DropQueueAsync(_testQueueName);

        // Act
        var result = await _sut.QueueExistsAsync(_testQueueName);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ReadAsync_should_read_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.ReadAsync<TestMessage>(_testQueueName);

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
        Assert.Null(msg.Headers);
    }

    [Fact]
    public async Task ReadAsync_should_read_message_with_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var headers = new Dictionary<string, object>()
        {
            { "CorrelationId", "abc-123" },
            { "Priority", 5 }
        };
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, headers);

        var msg = await _sut.ReadAsync<TestMessage>(_testQueueName);

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
        msg.Headers.ShouldDeepEqual(headers);
    }

    [Fact]
    public async Task ReadAsync_should_read_string_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.ReadAsync<string>(_testQueueName);

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(msgId, msg.MsgId);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
        Assert.True(msg.Vt > DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.Vt.Offset);
        Assert.Equal(1, msg.ReadCt);
        msg.Message.ShouldDeepEqual("{\"Bar\": \"Test\", \"Baz\": \"2023-09-01T01:23:45-04:00\", \"Foo\": 123}");
        Assert.Null(msg.Headers);
    }

    [Fact]
    public async Task ReadAsync_should_read_string_object_dictionary_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        var msg = await _sut.ReadAsync<Dictionary<string, object>>(_testQueueName);

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(msgId, msg.MsgId);
        Assert.True(msg.EnqueuedAt < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.EnqueuedAt.Offset);
        Assert.True(msg.Vt > DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, msg.Vt.Offset);
        Assert.Equal(1, msg.ReadCt);
        msg.Message.ShouldDeepEqual(new Dictionary<string, object>
        {
            { "Foo", 123 },
            { "Bar", "Test" },
            { "Baz", DateTimeOffset.Parse("2023-09-01T01:23:45-04:00") }
        });
        Assert.Null(msg.Headers);
    }

    [Fact]
    public async Task ReadAsync_should_return_null_if_no_message_is_available()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msg = await _sut.ReadAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Null(msg);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
    }

    [Fact]
    public async Task ReadBatchAsync_should_return_list_of_messages()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 })
        };

        var messages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);

        // Assert
        messages.Select(x => x.MsgId).OrderBy(x => x).ShouldDeepEqual(msgIds.OrderBy(x => x));
        messages.Select(x => x.Message).OrderBy(x => x!.Foo).ShouldDeepEqual(new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        });
        Assert.True(messages.All(x => x.Headers == null));
    }

    [Fact]
    public async Task ReadBatchAsync_should_return_list_of_messages_including_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }, new Dictionary<string, object> { { "header1", "aaa" } } ),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }, new Dictionary<string, object> { { "header1", "bbb" }, { "header2", 987 } }),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 }, new Dictionary<string, object> { { "header1", "ccc" } } )
        };

        var messages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);

        // Assert
        messages.Select(x => x.MsgId).OrderBy(x => x).ShouldDeepEqual(msgIds.OrderBy(x => x));
        messages.Select(x => x.Message).OrderBy(x => x!.Foo).ShouldDeepEqual(new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        });
        messages.Single(x => x.Message!.Foo == 1).Headers.ShouldDeepEqual(new Dictionary<string, object> { { "header1", "aaa" } });
        messages.Single(x => x.Message!.Foo == 2).Headers.ShouldDeepEqual(new Dictionary<string, object> { { "header1", "bbb" }, { "header2", 987 } });
        messages.Single(x => x.Message!.Foo == 3).Headers.ShouldDeepEqual(new Dictionary<string, object> { { "header1", "ccc" } });
    }

    [Fact]
    public async Task SendAsync_should_add_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_commit_with_database_transaction()
    {
        // Arrange
        await using var connection2 = await _postgresFixture.DataSource.OpenConnectionAsync();
        var sut = new NpgmqClient(connection2); // Don't use _sut here, as we want to test a new instance with a connection so we can create a transaction
        await sut.CreateQueueAsync(_testQueueName);

        // Act
        await using var transaction = await connection2.BeginTransactionAsync();
        var msgId = await sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await connection2.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));

        // Act
        await transaction.CommitAsync();

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_rollback_with_database_transaction()
    {
        // Arrange
        await using var connection2 = await _postgresFixture.DataSource.OpenConnectionAsync();
        var sut = new NpgmqClient(connection2); // Don't use _sut here, as we want to test a new instance with a connection so we can create a transaction
        await sut.CreateQueueAsync(_testQueueName);

        // Act
        await using var transaction = await connection2.BeginTransactionAsync();
        var msgId = await sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        });

        // Assert
        Assert.Equal(1, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE vt <= CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await connection2.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));

        // Act
        await transaction.RollbackAsync();

        // Assert
        Assert.Equal(0, await connection2.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
    }

    [Fact]
    public async Task SendAsync_should_add_string_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var message = "{\"Foo\": 123, \"Bar\": \"Test\", \"Baz\": \"2023-09-01T01:23:45-04:00\"}";
        var msgId = await _sut.SendAsync(_testQueueName, message);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
        var actualMessage = await _connection.ExecuteScalarAsync<string>($"SELECT message FROM pgmq.q_{_testQueueName} LIMIT 1;");
        JsonDocument.Parse(actualMessage!).ShouldDeepEqual(JsonDocument.Parse(message));
    }

    [Fact]
    public async Task SendAsync_should_add_string_object_dictionary_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var message = new Dictionary<string, object>()
        {
            { "Foo", 123 },
            { "Bar", "Test" },
            { "Baz", "2023-09-01T01:23:45-04:00" }
        };
        var msgId = await _sut.SendAsync(_testQueueName, message);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
        var actualMessage = await _connection.ExecuteScalarAsync<string>($"SELECT message FROM pgmq.q_{_testQueueName} LIMIT 1;");
        var actualDeserializedMessage = JsonSerializer.Deserialize<Dictionary<string, object>>(actualMessage!, DeserializationOptions);
        actualDeserializedMessage.ShouldDeepEqual(message);
    }

    [Fact]
    public async Task SendAsync_should_add_message_with_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var headers = new Dictionary<string, object>
        {
            { "some-header", 456 },
            { "another-header", "test" }
        };
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, headers);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE headers = '{{\"some-header\": 456, \"another-header\": \"test\"}}';"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_add_message_with_numeric_delay()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, 100);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE vt > CURRENT_TIMESTAMP;"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_add_message_with_timestamp_delay()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var delay = DateTimeOffset.Now.AddSeconds(100);
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, delay);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        var actualVt = await _connection.ExecuteScalarAsync<DateTime>($"SELECT vt FROM pgmq.q_{_testQueueName} LIMIT 1;");
        Assert.Equal(delay.TruncateToMicroseconds(), actualVt.ToDateTimeOffset().TruncateToMicroseconds());
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_add_message_with_numeric_delay_and_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var headers = new Dictionary<string, object>
        {
            { "some-header", 456 },
            { "another-header", "test" }
        };
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, headers, 100);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE vt > CURRENT_TIMESTAMP and headers = '{{\"some-header\": 456, \"another-header\": \"test\"}}';"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendAsync_should_add_message_with_timestamp_delay_and_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var headers = new Dictionary<string, object>
        {
            { "some-header", 456 },
            { "another-header", "test" }
        };
        var delay = DateTimeOffset.Now.AddSeconds(100);
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 123,
            Bar = "Test",
            Baz = DateTimeOffset.Parse("2023-09-01T01:23:45-04:00")
        }, headers, delay);

        // Assert
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        var actualVt = await _connection.ExecuteScalarAsync<DateTime>($"SELECT vt FROM pgmq.q_{_testQueueName} LIMIT 1;");
        Assert.Equal(delay.TruncateToMicroseconds(), actualVt.ToDateTimeOffset().TruncateToMicroseconds());
        Assert.Equal(1, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE headers = '{{\"some-header\": 456, \"another-header\": \"test\"}}';"));
        Assert.Equal(msgId, await _connection.ExecuteScalarAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} LIMIT 1;"));
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        };
        var msgIds = await _sut.SendBatchAsync(_testQueueName, messages);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages_with_headers()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        };
        var headers = new List<Dictionary<string, object>>
        {
            new() { { "header1", "aaa" } },
            new() { { "header1", "bbb" }, { "header2", 987 } },
            new() { { "header1", "ccc" } }
        };
        var msgIds = await _sut.SendBatchAsync(_testQueueName, messages, headers);

        // Assert
        var actualResults = (await _connection.QueryAsync<(long MsgId, string Message, string Headers)>($"SELECT msg_id, message, headers FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList();
        Assert.Equal(3, actualResults.Count);
        for (var i = 0; i < actualResults.Count; i++)
        {
            var actualResult = actualResults.ElementAt(i);

            var actualMsgId = actualResult.MsgId;
            var expectedMsgId = msgIds.ElementAt(i);
            Assert.Equal(expectedMsgId, actualMsgId);

            var actualMessage = JsonSerializer.Deserialize<TestMessage>(actualResult.Message);
            var expectedMessage = messages.ElementAt(i);
            actualMessage.ShouldDeepEqual(expectedMessage);

            var actualHeaders = JsonSerializer.Deserialize<Dictionary<string, object>>(actualResult.Headers, DeserializationOptions);
            var expectedHeaders = headers.ElementAt(i);
            actualHeaders.ShouldDeepEqual(expectedHeaders);
        }
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages_with_numeric_delay()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        };
        var msgIds = await _sut.SendBatchAsync(_testQueueName, messages, 100);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE message IS NOT NULL AND vt > CURRENT_TIMESTAMP;"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages_with_timestamp_delay()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        };
        var delay = DateTimeOffset.Now.AddSeconds(100);
        var msgIds = await _sut.SendBatchAsync(_testQueueName, messages, delay);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE message IS NOT NULL;"));
        var vts = await _connection.QueryAsync<DateTime>($"SELECT vt FROM pgmq.q_{_testQueueName};");
        Assert.True(vts.All(x => x.ToDateTimeOffset().TruncateToMicroseconds() == delay.TruncateToMicroseconds()));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages_with_headers_and_numeric_delay()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        };
        var headers = new List<Dictionary<string, object>>
        {
            new() { { "header1", "aaa" } },
            new() { { "header1", "bbb" }, { "header2", 987 } },
            new() { { "header1", "ccc" } }
        };
        var msgIds = await _sut.SendBatchAsync(_testQueueName, messages, headers, 100);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE message IS NOT NULL AND headers IS NOT NULL AND vt > CURRENT_TIMESTAMP;"));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task SendBatchAsync_should_add_multiple_messages_with_headers_and_timestamp_delay()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = new List<TestMessage>
        {
            new() { Foo = 1 },
            new() { Foo = 2 },
            new() { Foo = 3 }
        };
        var headers = new List<Dictionary<string, object>>
        {
            new() { { "header1", "aaa" } },
            new() { { "header1", "bbb" }, { "header2", 987 } },
            new() { { "header1", "ccc" } }
        };
        var delay = DateTimeOffset.Now.AddSeconds(100);
        var msgIds = await _sut.SendBatchAsync(_testQueueName, messages, headers, delay);

        // Assert
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
        Assert.Equal(3, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName} WHERE message IS NOT NULL AND headers IS NOT NULL;"));
        var vts = await _connection.QueryAsync<DateTime>($"SELECT vt FROM pgmq.q_{_testQueueName};");
        Assert.True(vts.All(x => x.ToDateTimeOffset().TruncateToMicroseconds() == delay.TruncateToMicroseconds()));
        Assert.Equal(msgIds.OrderBy(x => x), (await _connection.QueryAsync<long>($"SELECT msg_id FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList());
    }

    [Fact]
    public async Task SetVtAsync_should_change_vt_for_a_message()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 });
        var originalMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Equal(msgId, originalMessages.Single().MsgId);
        // Confirm there is nothing else to read
        var visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Empty(visibleMessages);

        // Act
        // Adjust the vt to be 60 seconds in the past, making the message available to read again.
        await _sut.SetVtAsync(_testQueueName, msgId, -60);
        visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Equal(msgId, visibleMessages.Single().MsgId);
        // After reading, there should be no more visible messages
        Assert.Empty(await _sut.ReadBatchAsync<TestMessage>(_testQueueName));
    }

    [SkippableFact]
    public async Task SetVtAsync_should_change_vt_for_a_message_using_timestamp()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.10.0"), "requires pgmq 1.10.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        var msgId = await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 });
        var originalMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Equal(msgId, originalMessages.Single().MsgId);
        // Confirm there is nothing else to read
        var visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Empty(visibleMessages);

        // Act
        var vt = DateTimeOffset.UtcNow.AddSeconds(-60).TruncateToMicroseconds();
        await _sut.SetVtAsync(_testQueueName, msgId, vt);
        var storedVt = (await _connection.QuerySingleAsync<DateTime>($"SELECT vt FROM pgmq.q_{_testQueueName} WHERE msg_id = @msgId;", new { msgId }))
            .ToDateTimeOffset()
            .TruncateToMicroseconds();
        visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Equal(vt, storedVt);
        Assert.Equal(msgId, visibleMessages.Single().MsgId);
        // After reading, there should be no more visible messages
        Assert.Empty(await _sut.ReadBatchAsync<TestMessage>(_testQueueName));
    }

    [SkippableFact]
    public async Task SetVtBatchAsync_should_change_vt_for_multiple_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.8.0"), "requires pgmq 1.8.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        var msgId1 = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 1
        });
        var msgId2 = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 2
        });
        var originalMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Equal(2, originalMessages.Count);
        // Confirm there is nothing else to read
        var visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Empty(visibleMessages);

        // Act
        // Adjust the vt to be 60 seconds in the past, making the messages available to read again.
        var expectedMsgIds = new List<long> { msgId1, msgId2 };
        var actualMsgIds = await _sut.SetVtBatchAsync(_testQueueName, expectedMsgIds, -60);
        visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Equal(expectedMsgIds, actualMsgIds);
        Assert.Equal(2, visibleMessages.Count);
        Assert.Contains(visibleMessages, msg => msg.MsgId == msgId1);
        Assert.Contains(visibleMessages, msg => msg.MsgId == msgId2);
        // After reading, there should be no more visible messages
        Assert.Empty(await _sut.ReadBatchAsync<TestMessage>(_testQueueName));
    }

    [SkippableFact]
    public async Task SetVtBatchAsync_should_change_vt_for_multiple_messages_using_timestamp()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.10.0"), "requires pgmq 1.10.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        var msgId1 = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 1
        });
        var msgId2 = await _sut.SendAsync(_testQueueName, new TestMessage
        {
            Foo = 2
        });
        var originalMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Equal(2, originalMessages.Count);
        // Confirm there is nothing else to read
        var visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
        Assert.Empty(visibleMessages);

        // Act
        var expectedMsgIds = new List<long> { msgId1, msgId2 };
        var vt = DateTimeOffset.UtcNow.AddSeconds(-60).TruncateToMicroseconds();
        var actualMsgIds = await _sut.SetVtBatchAsync(_testQueueName, expectedMsgIds, vt);
        var storedVts = (await _connection.QueryAsync<DateTime>($"SELECT vt FROM pgmq.q_{_testQueueName} WHERE msg_id = ANY(@msgIds) ORDER BY msg_id;", new { msgIds = expectedMsgIds }))
            .Select(x => x.ToDateTimeOffset().TruncateToMicroseconds())
            .ToList();
        visibleMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Equal(expectedMsgIds, actualMsgIds);
        Assert.All(storedVts, storedVt => Assert.Equal(vt, storedVt));
        Assert.Equal(2, visibleMessages.Count);
        Assert.Contains(visibleMessages, msg => msg.MsgId == msgId1);
        Assert.Contains(visibleMessages, msg => msg.MsgId == msgId2);
        // After reading, there should be no more visible messages
        Assert.Empty(await _sut.ReadBatchAsync<TestMessage>(_testQueueName));
    }

    [Fact]
    public async Task GetMetricsAsync_should_return_metrics_for_a_single_queue()
    {
        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        var metrics1 = await _sut.GetMetricsAsync(_testQueueName);
        await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 });
        await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 });
        var msgId3 = await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 });
        var msgId4 = await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 4 });
        await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 5 });
        await _sut.DeleteAsync(_testQueueName, msgId3);
        await _sut.ArchiveAsync(_testQueueName, msgId4);

        // Act
        var metrics2 = await _sut.GetMetricsAsync(_testQueueName);
        await _sut.ReadAsync<string>(_testQueueName);
        var metrics3 = await _sut.GetMetricsAsync(_testQueueName);
        await _sut.PurgeQueueAsync(_testQueueName);
        var metrics4 = await _sut.GetMetricsAsync(_testQueueName);

        // Assert
        Assert.Equal(_testQueueName, metrics1.QueueName);
        Assert.Equal(0, metrics1.QueueLength);
        Assert.Null(metrics1.NewestMessageAge);
        Assert.Null(metrics1.OldestMessageAge);
        Assert.Equal(0, metrics1.TotalMessages);
        Assert.True(metrics1.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);

        Assert.Equal(_testQueueName, metrics2.QueueName);
        Assert.Equal(3, metrics2.QueueLength);
        Assert.True(metrics2.NewestMessageAge >= 0);
        Assert.True(metrics2.OldestMessageAge >= 0);
        Assert.Equal(5, metrics2.TotalMessages);
        Assert.True(metrics2.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);

        Assert.Equal(_testQueueName, metrics3.QueueName);
        Assert.Equal(3, metrics3.QueueLength);
        Assert.True(metrics3.NewestMessageAge >= 0);
        Assert.True(metrics3.OldestMessageAge >= 0);
        Assert.Equal(5, metrics3.TotalMessages);
        Assert.True(metrics3.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);

        Assert.Equal(_testQueueName, metrics4.QueueName);
        Assert.Equal(0, metrics4.QueueLength);
        Assert.Null(metrics1.NewestMessageAge);
        Assert.Null(metrics1.OldestMessageAge);
        Assert.Equal(5, metrics4.TotalMessages);
        Assert.True(metrics4.ScrapeTime < DateTimeOffset.UtcNow);
        Assert.Equal(TimeSpan.Zero, metrics1.ScrapeTime.Offset);
    }

    [Fact]
    public async Task GetMetricsAsync_should_return_metrics_for_all_queues()
    {
        // Create some queues just for testing this function.
        var testMetricsQueueName1 = _testQueueName + "_m1";
        var testMetricsQueueName2 = _testQueueName + "_m2";
        var testMetricsQueueName3 = _testQueueName + "_m3";
        try
        {
            // Arrange
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
            // Cleanup
            try { await _sut.DropQueueAsync(testMetricsQueueName1); } catch { /* ignored */ }
            try { await _sut.DropQueueAsync(testMetricsQueueName2); } catch { /* ignored */ }
            try { await _sut.DropQueueAsync(testMetricsQueueName3); } catch { /* ignored */ }
        }
    }
}