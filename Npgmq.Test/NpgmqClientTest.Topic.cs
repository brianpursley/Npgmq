using System.Text.Json;
using Dapper;
using DeepEqual.Syntax;

namespace Npgmq.Test;

public partial class NpgmqClientTest
{
    [SkippableFact]
    public async Task BindTopicAsync_should_be_idempotent_and_list_topic_bindings()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        await _sut.BindTopicAsync("orders.*", _testQueueName);
        await _sut.BindTopicAsync("orders.*", _testQueueName);
        await _sut.BindTopicAsync("orders.created", _testQueueName);

        var allBindings = await _sut.ListTopicBindingsAsync();
        var queueBindings = await _sut.ListTopicBindingsAsync(_testQueueName);

        // Assert
        Assert.Equal(2, queueBindings.Count);
        Assert.Equal(2, allBindings.Count(x => x.QueueName == _testQueueName));

        var bindingsByPattern = queueBindings.ToDictionary(x => x.Pattern);
        Assert.Equal(["orders.*", "orders.created"], bindingsByPattern.Keys.Order().ToArray());

        Assert.All(bindingsByPattern.Values, binding =>
        {
            Assert.Equal(_testQueueName, binding.QueueName);
            Assert.NotEqual(default, binding.BoundAt);
            Assert.False(string.IsNullOrWhiteSpace(binding.CompiledRegex));
        });
    }

    [SkippableFact]
    public async Task UnbindTopicAsync_should_remove_binding_and_return_false_when_it_does_not_exist()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("orders.#", _testQueueName);

        // Act
        var removed = await _sut.UnbindTopicAsync("orders.#", _testQueueName);
        var removedAgain = await _sut.UnbindTopicAsync("orders.#", _testQueueName);
        var remainingBindings = await _sut.ListTopicBindingsAsync(_testQueueName);

        // Assert
        Assert.True(removed);
        Assert.False(removedAgain);
        Assert.Empty(remainingBindings);
    }

    [SkippableFact]
    public async Task SendTopicAsync_should_route_message_to_all_matching_queues_and_return_queue_count()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        var secondaryQueueName = _testQueueName + "_secondary";

        try
        {
            // Arrange
            await _sut.CreateQueueAsync(_testQueueName);
            await _sut.CreateQueueAsync(secondaryQueueName);
            await _sut.BindTopicAsync("orders.#", _testQueueName);
            await _sut.BindTopicAsync("orders.created", secondaryQueueName);

            var message = new TestMessage
            {
                Foo = 123,
                Bar = "created"
            };

            // Act
            var result = await _sut.SendTopicAsync("orders.created", message);

            var primaryQueueMessage = await _sut.ReadAsync<TestMessage>(_testQueueName);
            var secondaryQueueMessage = await _sut.ReadAsync<TestMessage>(secondaryQueueName);

            // Assert
            Assert.Equal(2, result);

            Assert.NotNull(primaryQueueMessage);
            Assert.NotNull(secondaryQueueMessage);

            primaryQueueMessage.Message.ShouldDeepEqual(message);
            secondaryQueueMessage.Message.ShouldDeepEqual(message);

            Assert.Null(primaryQueueMessage.Headers);
            Assert.Null(secondaryQueueMessage.Headers);
        }
        finally
        {
            // Cleanup
            try { await _sut.DropQueueAsync(secondaryQueueName); } catch { /* ignored */ }
        }
    }

    [SkippableFact]
    public async Task SendTopicAsync_should_return_zero_when_no_bindings_match()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("orders.#", _testQueueName);

        // Act
        var result = await _sut.SendTopicAsync("payments.failed", new TestMessage { Foo = 123 });

        // Assert
        Assert.Equal(0, result);
        Assert.Equal(0, await _connection.ExecuteScalarAsync<long>($"SELECT count(*) FROM pgmq.q_{_testQueueName};"));
    }

    [SkippableFact]
    public async Task SendTopicAsync_should_add_message_with_numeric_delay()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.email", _testQueueName);

        var message = new TestMessage
        {
            Foo = 123,
            Bar = "delayed"
        };

        // Act
        var result = await _sut.SendTopicAsync("notifications.email", message, 60);

        var actualRow = await _connection.QuerySingleAsync<(string Message, string Headers, DateTime Vt)>(
            $"SELECT message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} LIMIT 1;");

        // Assert
        Assert.Equal(1, result);

        JsonSerializer.Deserialize<TestMessage>(actualRow.Message).ShouldDeepEqual(message);
        Assert.Null(actualRow.Headers);
        Assert.True(actualRow.Vt.ToDateTimeOffset() > DateTimeOffset.UtcNow);
    }

    [SkippableFact]
    public async Task SendTopicAsync_should_add_message_with_headers_and_numeric_delay()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.email", _testQueueName);

        var headers = new Dictionary<string, object>
        {
            { "some-header", 456 },
            { "another-header", "test" }
        };
        var message = new TestMessage
        {
            Foo = 123,
            Bar = "delayed"
        };

        // Act
        var result = await _sut.SendTopicAsync("notifications.email", message, headers, 60);

        var actualRow = await _connection.QuerySingleAsync<(string Message, string Headers, DateTime Vt)>(
            $"SELECT message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} LIMIT 1;");

        // Assert
        Assert.Equal(1, result);

        JsonSerializer.Deserialize<TestMessage>(actualRow.Message).ShouldDeepEqual(message);
        JsonSerializer.Deserialize<Dictionary<string, object>>(actualRow.Headers, DeserializationOptions)
            .ShouldDeepEqual(headers);
        Assert.True(actualRow.Vt.ToDateTimeOffset() > DateTimeOffset.UtcNow);
    }

    [SkippableFact]
    public async Task SendBatchTopicAsync_should_route_messages_to_all_matching_queues_and_return_results_for_each_queue()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        var secondaryQueueName = _testQueueName + "_secondary";

        try
        {
            // Arrange
            await _sut.CreateQueueAsync(_testQueueName);
            await _sut.CreateQueueAsync(secondaryQueueName);
            await _sut.BindTopicAsync("orders.#", _testQueueName);
            await _sut.BindTopicAsync("orders.created", secondaryQueueName);

            var messages = new List<TestMessage>
            {
                new() { Foo = 1 },
                new() { Foo = 2 }
            };

            // Act
            var results = await _sut.SendBatchTopicAsync("orders.created", messages);

            var primaryQueueMessages = await _sut.ReadBatchAsync<TestMessage>(_testQueueName);
            var secondaryQueueMessages = await _sut.ReadBatchAsync<TestMessage>(secondaryQueueName);

            // Assert
            Assert.Equal(4, results.Count);
            Assert.Equal(2, results.Count(x => x.QueueName == _testQueueName));
            Assert.Equal(2, results.Count(x => x.QueueName == secondaryQueueName));

            primaryQueueMessages.Select(x => x.Message).OrderBy(x => x!.Foo)
                .ShouldDeepEqual(messages.OrderBy(x => x.Foo));
            secondaryQueueMessages.Select(x => x.Message).OrderBy(x => x!.Foo)
                .ShouldDeepEqual(messages.OrderBy(x => x.Foo));

            primaryQueueMessages.Select(x => x.MsgId).OrderBy(x => x)
                .ShouldDeepEqual(results.Where(x => x.QueueName == _testQueueName).Select(x => x.MsgId).OrderBy(x => x));
            secondaryQueueMessages.Select(x => x.MsgId).OrderBy(x => x)
                .ShouldDeepEqual(results.Where(x => x.QueueName == secondaryQueueName).Select(x => x.MsgId).OrderBy(x => x));
        }
        finally
        {
            // Cleanup
            try { await _sut.DropQueueAsync(secondaryQueueName); } catch { /* ignored */ }
        }
    }
    [SkippableFact]
    public async Task SendBatchTopicAsync_should_add_messages_with_headers()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.#", _testQueueName);

        var messages = new List<TestMessage>
        {
            new() { Foo = 1, Bar = "a" },
            new() { Foo = 2, Bar = "b" }
        };
        var headers = new List<Dictionary<string, object>>
        {
            new() { { "header1", "aaa" } },
            new() { { "header1", "bbb" }, { "header2", 987 } }
        };

        // Act
        var results = await _sut.SendBatchTopicAsync("notifications.email", messages, headers);

        var actualRows = (await _connection.QueryAsync<(long MsgId, string Message, string Headers, DateTime Vt)>(
            $"SELECT msg_id AS MsgId, message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(2, actualRows.Count);

        Assert.Equal(results.Select(x => x.MsgId).OrderBy(x => x), actualRows.Select(x => x.MsgId).OrderBy(x => x));

        for (var i = 0; i < actualRows.Count; i++)
        {
            JsonSerializer.Deserialize<TestMessage>(actualRows[i].Message).ShouldDeepEqual(messages[i]);
            JsonSerializer.Deserialize<Dictionary<string, object>>(actualRows[i].Headers, DeserializationOptions)
                .ShouldDeepEqual(headers[i]);
        }
    }

    [SkippableFact]
    public async Task SendBatchTopicAsync_should_add_messages_with_timestamp_delay()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.#", _testQueueName);

        var messages = new List<TestMessage>
        {
            new() { Foo = 1, Bar = "a" },
            new() { Foo = 2, Bar = "b" }
        };
        var delay = DateTimeOffset.UtcNow.AddSeconds(60).TruncateToMicroseconds();

        // Act
        var results = await _sut.SendBatchTopicAsync("notifications.email", messages, delay);

        var actualRows = (await _connection.QueryAsync<(long MsgId, string Message, string Headers, DateTime Vt)>(
            $"SELECT msg_id AS MsgId, message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(2, actualRows.Count);

        Assert.Equal(results.Select(x => x.MsgId).OrderBy(x => x), actualRows.Select(x => x.MsgId).OrderBy(x => x));
        Assert.True(actualRows.All(x => x.Vt.ToDateTimeOffset().TruncateToMicroseconds() == delay));

        for (var i = 0; i < actualRows.Count; i++)
        {
            JsonSerializer.Deserialize<TestMessage>(actualRows[i].Message).ShouldDeepEqual(messages[i]);
            Assert.Null(actualRows[i].Headers);
        }
    }

    [SkippableFact]
    public async Task SendBatchTopicAsync_should_add_messages_with_numeric_delay()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.#", _testQueueName);

        var messages = new List<TestMessage>
        {
            new() { Foo = 1, Bar = "a" },
            new() { Foo = 2, Bar = "b" }
        };

        // Act
        var results = await _sut.SendBatchTopicAsync("notifications.email", messages, 60);

        var actualRows = (await _connection.QueryAsync<(long MsgId, string Message, string Headers, DateTime Vt)>(
            $"SELECT msg_id AS MsgId, message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(2, actualRows.Count);

        Assert.Equal(results.Select(x => x.MsgId).OrderBy(x => x), actualRows.Select(x => x.MsgId).OrderBy(x => x));
        Assert.True(actualRows.All(x => x.Vt.ToDateTimeOffset() > DateTimeOffset.UtcNow));

        for (var i = 0; i < actualRows.Count; i++)
        {
            JsonSerializer.Deserialize<TestMessage>(actualRows[i].Message).ShouldDeepEqual(messages[i]);
            Assert.Null(actualRows[i].Headers);
        }
    }

    [SkippableFact]
    public async Task SendBatchTopicAsync_should_add_messages_with_headers_and_timestamp_delay()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.#", _testQueueName);

        var messages = new List<TestMessage>
        {
            new() { Foo = 1, Bar = "a" },
            new() { Foo = 2, Bar = "b" }
        };
        var headers = new List<Dictionary<string, object>>
        {
            new() { { "header1", "aaa" } },
            new() { { "header1", "bbb" }, { "header2", 987 } }
        };
        var delay = DateTimeOffset.UtcNow.AddSeconds(60).TruncateToMicroseconds();

        // Act
        var results = await _sut.SendBatchTopicAsync("notifications.email", messages, headers, delay);

        var actualRows = (await _connection.QueryAsync<(long MsgId, string Message, string Headers, DateTime Vt)>(
            $"SELECT msg_id AS MsgId, message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(2, actualRows.Count);

        Assert.Equal(results.Select(x => x.MsgId).OrderBy(x => x), actualRows.Select(x => x.MsgId).OrderBy(x => x));
        Assert.True(actualRows.All(x => x.Vt.ToDateTimeOffset().TruncateToMicroseconds() == delay));

        for (var i = 0; i < actualRows.Count; i++)
        {
            JsonSerializer.Deserialize<TestMessage>(actualRows[i].Message).ShouldDeepEqual(messages[i]);
            JsonSerializer.Deserialize<Dictionary<string, object>>(actualRows[i].Headers, DeserializationOptions)
                .ShouldDeepEqual(headers[i]);
        }
    }

    [SkippableFact]
    public async Task SendBatchTopicAsync_should_add_messages_with_headers_and_numeric_delay()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.0"), "requires pgmq 1.11.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);
        await _sut.BindTopicAsync("notifications.#", _testQueueName);

        var messages = new List<TestMessage>
        {
            new() { Foo = 1, Bar = "a" },
            new() { Foo = 2, Bar = "b" }
        };
        var headers = new List<Dictionary<string, object>>
        {
            new() { { "header1", "aaa" } },
            new() { { "header1", "bbb" }, { "header2", 987 } }
        };

        // Act
        var results = await _sut.SendBatchTopicAsync("notifications.email", messages, headers, 60);

        var actualRows = (await _connection.QueryAsync<(long MsgId, string Message, string Headers, DateTime Vt)>(
            $"SELECT msg_id AS MsgId, message AS Message, headers AS Headers, vt AS Vt FROM pgmq.q_{_testQueueName} ORDER BY msg_id;")).ToList();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(2, actualRows.Count);

        Assert.Equal(results.Select(x => x.MsgId).OrderBy(x => x), actualRows.Select(x => x.MsgId).OrderBy(x => x));
        Assert.True(actualRows.All(x => x.Vt.ToDateTimeOffset() > DateTimeOffset.UtcNow));

        for (var i = 0; i < actualRows.Count; i++)
        {
            JsonSerializer.Deserialize<TestMessage>(actualRows[i].Message).ShouldDeepEqual(messages[i]);
            JsonSerializer.Deserialize<Dictionary<string, object>>(actualRows[i].Headers, DeserializationOptions)
                .ShouldDeepEqual(headers[i]);
        }
    }
}