using System.Reflection;
using Microsoft.Extensions.Configuration;
using Npgmq;
using Npgsql;

const string defaultConnectionString = "Host=localhost;Username=postgres;Password=postgres;Database=npgmq_test;";

var configuration = new ConfigurationBuilder()
    .AddEnvironmentVariables()
    .AddUserSecrets(Assembly.GetExecutingAssembly())
    .Build();

var connectionString = configuration.GetConnectionString("ExampleDB") ?? defaultConnectionString;

// Test Npgmq with connection string
{
    Console.WriteLine("NpgmqClient using a connection string...");
    var npgmq = new NpgmqClient(connectionString);

    await npgmq.InitAsync();
    await npgmq.CreateQueueAsync("example_queue");

    var msgId = await npgmq.SendAsync("example_queue", new MyMessageType
    {
        Foo = "Connection string test",
        Bar = 1
    });
    Console.WriteLine($"Sent message with id {msgId}");

    var msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
    if (msg != null)
    {
        Console.WriteLine($"Read message with id {msg.MsgId} (EnqueuedAt = {msg.EnqueuedAt}, Vt = {msg.Vt}): Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
        await npgmq.ArchiveAsync("example_queue", msg.MsgId);
    }
    Console.WriteLine();
}

// Test Npgmq with data source
{
    Console.WriteLine("NpgmqClient using a data source...");

    await using var dataSource = NpgsqlDataSource.Create(connectionString);
    var npgmq = new NpgmqClient(dataSource);

    await npgmq.InitAsync();
    await npgmq.CreateQueueAsync("example_queue");

    var msgId = await npgmq.SendAsync("example_queue", new MyMessageType
    {
        Foo = "Connection string test",
        Bar = 1
    });
    Console.WriteLine($"Sent message with id {msgId}");

    var msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
    if (msg != null)
    {
        Console.WriteLine($"Read message with id {msg.MsgId} (EnqueuedAt = {msg.EnqueuedAt}, Vt = {msg.Vt}): Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
        await npgmq.ArchiveAsync("example_queue", msg.MsgId);
    }
    Console.WriteLine();
}

// Test Npgmq with connection object and a transaction
{
    Console.WriteLine("NpgmqClient using a connection object with a transaction...");
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    var npgmq = new NpgmqClient(connection);

    await using (var tx = await connection.BeginTransactionAsync())
    {
        var msgId = await npgmq.SendAsync("example_queue", new MyMessageType
        {
            Foo = "Connection object test",
            Bar = 2
        });
        Console.WriteLine($"Sent message with id {msgId}");
        msgId = await npgmq.SendAsync("example_queue", new MyMessageType
        {
            Foo = "Connection object test",
            Bar = 3
        });
        Console.WriteLine($"Sent message with id {msgId}");

        await tx.CommitAsync();
    }

    var msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
    if (msg != null)
    {
        Console.WriteLine($"Read message with id {msg.MsgId} (EnqueuedAt = {msg.EnqueuedAt}, Vt = {msg.Vt}): Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
        await npgmq.ArchiveAsync("example_queue", msg.MsgId);
    }
    msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
    if (msg != null)
    {
        Console.WriteLine($"Read message with id {msg.MsgId} (EnqueuedAt = {msg.EnqueuedAt}, Vt = {msg.Vt}): Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
        await npgmq.ArchiveAsync("example_queue", msg.MsgId);
    }
    Console.WriteLine();
}

internal class MyMessageType
{
    public string Foo { get; set; } = null!;
    public int Bar { get; set; }
}