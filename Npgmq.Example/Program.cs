using System.Reflection;
using Microsoft.Extensions.Configuration;
using Npgmq;

var configuration = new ConfigurationBuilder()
    .AddEnvironmentVariables()
    .AddUserSecrets(Assembly.GetExecutingAssembly())
    .Build();

var npgmq = new NpgmqClient(configuration.GetConnectionString("ExampleDB")!);

await npgmq.InitAsync();
await npgmq.CreateQueueAsync("example_queue");

var msgId = await npgmq.SendAsync("example_queue", new MyMessageType
{
    Foo = "Test",
    Bar = 123
});
Console.WriteLine($"Sent message with id {msgId}");

var msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
if (msg != null)
{
    Console.WriteLine($"Read message with id {msg.MsgId}: Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
    await npgmq.ArchiveAsync("example_queue", msg.MsgId);
}

internal class MyMessageType
{
    public string Foo { get; set; } = null!;
    public int Bar { get; set; }
}