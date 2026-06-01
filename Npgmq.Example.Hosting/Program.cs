using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgmq;
using Npgmq.Example.Hosting;

var host = Host.CreateApplicationBuilder(args);

host.Services.AddNpgmqClient(
    "Host=localhost; Port=2401; Database=postgres; Username=postgres; Password=password;"
);

host.Services.AddNpgmqHosting(options =>
{
    options.UseQueue("test_queue", queue =>
    {
        queue.WithHandler<SampleEvent, SampleEventHandler>();
    });
});

host.Services.AddHostedService<EventPublisherBackgroundTask>();

var app = host.Build();

await app.RunAsync();