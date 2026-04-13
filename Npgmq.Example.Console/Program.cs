using System.Reflection;
using Microsoft.Extensions.Configuration;
using Npgmq;
using Npgsql;

await EnsureNpgmqInitializedAsync();
await DemoNpgmqClientWithConnectionStringAsync();
await DemoNpgmqClientWithDataSourceAsync();
await DemoNpgmqClientWithConnectionAndTransactionAsync();
await DemoNpgmqClientTopicRoutingAsync();
return;

async Task EnsureNpgmqInitializedAsync()
{
    var npgmq = new NpgmqClient(GetConnectionString());
    await npgmq.InitAsync();
}

async Task DemoNpgmqClientWithConnectionStringAsync()
{
    Console.WriteLine("Demoing NpgmqClient using a connection string...");
    var npgmq = new NpgmqClient(GetConnectionString());

    await npgmq.CreateQueueAsync("example_queue");

    var msgId = await npgmq.SendAsync("example_queue", new MyMessageType("Connection string test", 1));
    Console.WriteLine($"Sent message with id {msgId}");

    var msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
    if (msg != null)
    {
        Console.WriteLine($"Read message with id {msg.MsgId} (EnqueuedAt = {msg.EnqueuedAt}, Vt = {msg.Vt}): Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
        await npgmq.ArchiveAsync("example_queue", msg.MsgId);
    }
    Console.WriteLine();
}

async Task DemoNpgmqClientWithDataSourceAsync()
{
    Console.WriteLine("Demoing NpgmqClient using a data source...");

    await using var dataSource = NpgsqlDataSource.Create(GetConnectionString());
    var npgmq = new NpgmqClient(dataSource);

    await npgmq.CreateQueueAsync("example_queue");

    var msgId = await npgmq.SendAsync("example_queue", new MyMessageType("Data source test", 2));
    Console.WriteLine($"Sent message with id {msgId}");

    var msg = await npgmq.ReadAsync<MyMessageType>("example_queue");
    if (msg != null)
    {
        Console.WriteLine($"Read message with id {msg.MsgId} (EnqueuedAt = {msg.EnqueuedAt}, Vt = {msg.Vt}): Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
        await npgmq.ArchiveAsync("example_queue", msg.MsgId);
    }
    Console.WriteLine();
}

async Task DemoNpgmqClientWithConnectionAndTransactionAsync()
{
    Console.WriteLine("Demoing NpgmqClient using a connection object with a transaction...");
    await using var connection = new NpgsqlConnection(GetConnectionString());
    await connection.OpenAsync();
    var npgmq = new NpgmqClient(connection);

    await using (var tx = await connection.BeginTransactionAsync())
    {
        var msgId = await npgmq.SendAsync("example_queue", new MyMessageType("Connection object with transaction test", 3));
        Console.WriteLine($"Sent message with id {msgId}");
        msgId = await npgmq.SendAsync("example_queue", new MyMessageType("Connection object with transaction test", 4));
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

async Task DemoNpgmqClientTopicRoutingAsync()
{
    Console.WriteLine("Demoing topic routing...");
    var npgmq = new NpgmqClient(GetConnectionString());

    if ((await npgmq.GetPgmqVersionAsync()) < new Version(1, 11))
    {
        Console.WriteLine("SKIPPED: Topic routing requires pgmq 1.11.0 or later");
        return;
    }

    var queueNames = new[] { "all_logs", "error_logs", "api_errors" };
    foreach (string queueName in queueNames)
    {
        await npgmq.CreateQueueAsync(queueName);
    }

    await npgmq.BindTopicAsync("logs.#", "all_logs");
    await npgmq.BindTopicAsync("logs.*.error", "error_logs");
    await npgmq.BindTopicAsync("logs.api.error", "api_errors");

    var topicBindings = await npgmq.ListTopicBindingsAsync();
    Console.WriteLine($"There are {topicBindings.Count} topic bindings");
    foreach (var topicBinding in topicBindings)
    {
        Console.WriteLine($"Topic pattern '{topicBinding.Pattern}' is bound to queue '{topicBinding.QueueName}'");
    }

    await npgmq.SendTopicAsync("logs.api.error", new MyLogMessageRecord("API failed"));
    await npgmq.SendTopicAsync("logs.db.error", new MyLogMessageRecord("DB connection failed"));
    await npgmq.SendTopicAsync("logs.api.info", new MyLogMessageRecord("Request received"));

    foreach (string queueName in queueNames)
    {
        var messages = await npgmq.ReadBatchAsync<MyLogMessageRecord>(queueName);
        Console.WriteLine($"Queue {queueName} read {messages.Count} messages");
        foreach (var m in messages)
        {
            Console.WriteLine($"Queue {queueName} read log record with text: {m.Message?.Text}");
            await npgmq.ArchiveAsync(queueName, m.MsgId);
        }
    }

    await npgmq.UnbindTopicAsync("logs.#", "all_logs");
    await npgmq.UnbindTopicAsync("logs.*.error", "error_logs");
    await npgmq.UnbindTopicAsync("logs.api.error", "api_errors");

    topicBindings = await npgmq.ListTopicBindingsAsync();
    Console.WriteLine($"There are {topicBindings.Count} topic bindings");

    Console.WriteLine();
}

string GetConnectionString()
{
    const string defaultConnectionString = "Host=localhost;Username=postgres;Password=postgres;Database=npgmq_test;";

    var configuration = new ConfigurationBuilder()
        .AddEnvironmentVariables()
        .AddUserSecrets(Assembly.GetExecutingAssembly())
        .Build();

    return configuration.GetConnectionString("ExampleDB") ?? defaultConnectionString;
}

internal record MyMessageType(string Foo, int Bar);
internal record MyLogMessageRecord(string Text);