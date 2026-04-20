using System.Collections.Concurrent;

namespace Npgmq.Test;

[Collection("Npgmq")]
public class NpgmqClientStressTest(PostgresFixture postgresFixture) : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly NpgmqClient _sut = new(postgresFixture.DataSource);
    private readonly string _testQueueName = $"test_{Guid.NewGuid():N}";

    private sealed record WorkItem(int Id);

    public async Task InitializeAsync()
    {
        await _sut.CreateQueueAsync(_testQueueName);
    }

    public async Task DisposeAsync()
    {
        if (await _sut.QueueExistsAsync(_testQueueName))
        {
            await _sut.DropQueueAsync(_testQueueName);
        }
    }

    [Fact]
    public async Task Stress_SendAsync_PopAsync()
    {
        // Arrange
        const int totalMessages = 20_000;
        const int producerCount = 40; // Keep total producer + consumer count below postgres max connections
        const int consumerCount = 40;

        var seen = new ConcurrentDictionary<int, int>(); // id -> count
        var produced = Enumerable.Range(1, totalMessages).ToArray();

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

        // Act
        var producerTasks = Enumerable.Range(0, producerCount).Select(async p =>
        {
            // Simple partitioning
            for (int i = p; i < produced.Length; i += producerCount)
            {
                await _sut.SendAsync(_testQueueName, new WorkItem(produced[i]), cts.Token);
            }
        }).ToArray();

        var consumerTasks = Enumerable.Range(0, consumerCount).Select(async _ =>
        {
            while (!cts.IsCancellationRequested && seen.Count < totalMessages)
            {
                var msg = await _sut.PopAsync<WorkItem>(_testQueueName, cts.Token);
                if (msg == null)
                {
                    await Task.Delay(10, cts.Token);
                    continue;
                }

                var id = msg.Message!.Id;
                seen.AddOrUpdate(id, 1, (_, old) => old + 1);
            }
        }).ToArray();

        await Task.WhenAll(producerTasks);
        await Task.WhenAll(consumerTasks);

        // Assert
        Assert.Equal(totalMessages, seen.Count);
        Assert.DoesNotContain(seen, kvp => kvp.Value != 1);
        Assert.Null(await _sut.ReadAsync<WorkItem>(_testQueueName, cancellationToken: CancellationToken.None));
    }

    [Fact]
    public async Task Stress_SendAsync_ReadAsync_ArchiveAsync()
    {
        // Arrange
        const int totalMessages = 20_000;
        const int producerCount = 40; // Keep total producer + consumer count below postgres max connections
        const int consumerCount = 40;

        var seen = new ConcurrentDictionary<int, int>(); // id -> count
        var produced = Enumerable.Range(1, totalMessages).ToArray();

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

        // Act
        var producerTasks = Enumerable.Range(0, producerCount).Select(async p =>
        {
            // Simple partitioning
            for (int i = p; i < produced.Length; i += producerCount)
            {
                await _sut.SendAsync(_testQueueName, new WorkItem(produced[i]), cts.Token);
            }
        }).ToArray();

        var consumerTasks = Enumerable.Range(0, consumerCount).Select(async _ =>
        {
            while (!cts.IsCancellationRequested && seen.Count < totalMessages)
            {
                var msg = await _sut.ReadAsync<WorkItem>(_testQueueName, cancellationToken: cts.Token);
                if (msg == null)
                {
                    await Task.Delay(10, cts.Token);
                    continue;
                }

                Assert.True(await _sut.ArchiveAsync(_testQueueName, msg.MsgId, cts.Token));

                var id = msg.Message!.Id;
                seen.AddOrUpdate(id, 1, (_, old) => old + 1);
            }
        }).ToArray();

        await Task.WhenAll(producerTasks);
        await Task.WhenAll(consumerTasks);

        // Assert
        Assert.Equal(totalMessages, seen.Count);
        Assert.DoesNotContain(seen, kvp => kvp.Value != 1);
        Assert.Null(await _sut.ReadAsync<WorkItem>(_testQueueName, cancellationToken: CancellationToken.None));
    }
}