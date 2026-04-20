using DeepEqual.Syntax;

namespace Npgmq.Test;

public partial class NpgmqClientTest
{
    [SkippableFact]
    public async Task PollGroupedAsync_should_wait_for_multiple_grouped_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.9.0"), "requires pgmq 1.9.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var pollTask = _sut.PollGroupedAsync<TestMessage>(_testQueueName, limit: 3);

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
            ],
            [
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")
            ]
        );

        // Get the messages received by the poll
        var messages = await pollTask;

        // Assert
        messages.Select(m => m.MsgId).ShouldDeepEqual(new[] { msgIds[0], msgIds[3], msgIds[4] });
        Assert.All(messages, m => Assert.NotNull(m.Headers));
    }

    [SkippableFact]
    public async Task PollGroupedRoundRobinAsync_should_wait_for_multiple_grouped_messages_using_round_robin()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.9.0"), "requires pgmq 1.9.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var pollTask = _sut.PollGroupedRoundRobinAsync<TestMessage>(_testQueueName, limit: 3);

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
            ],
            [
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2"),
                NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2")
            ]
        );

        // Get the messages received by the poll
        var messages = await pollTask;

        // Assert
        messages.Select(m => m.MsgId).ShouldDeepEqual(new[] { msgIds[0], msgIds[3], msgIds[1] });
        Assert.All(messages, m => Assert.NotNull(m.Headers));
    }

    [SkippableFact]
    public async Task ReadGroupedAsync_should_read_grouped_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.9.0"), "requires pgmq 1.9.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 4 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 5 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1"))
        };
        var messages = await _sut.ReadGroupedAsync<TestMessage>(_testQueueName, limit: 3);

        // Assert
        messages.Select(x => x.MsgId).ShouldDeepEqual(new[] { msgIds[0], msgIds[3], msgIds[4] });
        Assert.True(messages.All(x => x.Headers != null));
    }

    [SkippableFact]
    public async Task ReadGroupedAsync_should_handle_no_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.9.0"), "requires pgmq 1.9.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = await _sut.ReadGroupedAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Empty(messages);
    }

    [SkippableFact]
    public async Task ReadGroupedRoundRobinAsync_should_read_grouped_messages_using_round_robin()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.9.0"), "requires pgmq 1.9.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 4 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 5 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2"))
        };
        var messages = await _sut.ReadGroupedRoundRobinAsync<TestMessage>(_testQueueName, limit: 3);

        // Assert
        messages.Select(x => x.MsgId).ShouldDeepEqual(new[] { msgIds[0], msgIds[3], msgIds[1] });
        Assert.All(messages, m => Assert.NotNull(m.Headers));
    }

    [SkippableFact]
    public async Task ReadGroupedRoundRobinAsync_should_handle_no_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.9.0"), "requires pgmq 1.9.0 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = await _sut.ReadGroupedRoundRobinAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Empty(messages);
    }

    [SkippableFact]
    public async Task ReadGroupedHeadAsync_should_read_grouped_head_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.1"), "requires pgmq 1.11.1 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var msgIds = new List<long>
        {
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 1 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 2 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 3 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group1")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 4 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2")),
            await _sut.SendAsync(_testQueueName, new TestMessage { Foo = 5 }, NpgmqHeaders.From(NpgmqHeaders.XPgmqGroup, "group2"))
        };
        var messages = await _sut.ReadGroupedHeadAsync<TestMessage>(_testQueueName);

        // Assert
        messages.Select(x => x.MsgId).ShouldDeepEqual(new[] { msgIds[0], msgIds[3] });
        Assert.All(messages, m => Assert.NotNull(m.Headers));
    }

    [SkippableFact]
    public async Task ReadGroupedHeadAsync_should_handle_no_messages()
    {
        Skip.IfNot(await IsMinPgmqVersion("1.11.1"), "requires pgmq 1.11.1 or later.");

        // Arrange
        await _sut.CreateQueueAsync(_testQueueName);

        // Act
        var messages = await _sut.ReadGroupedHeadAsync<TestMessage>(_testQueueName);

        // Assert
        Assert.Empty(messages);
    }
}