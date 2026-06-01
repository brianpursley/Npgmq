using Microsoft.Extensions.Hosting;

namespace Npgmq.Example.Hosting;

internal sealed class EventPublisherBackgroundTask(NpgmqClient client) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            
            await client.SendAsync(
                "test_queue", 
                new SampleEvent(), 
                stoppingToken);
        }
    }
}