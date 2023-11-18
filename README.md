# Npgsql

A .NET client for <a href="https://github.com/tembo-io/pgmq">Postgres Message Queue</a> (PGMQ).

## Requirements

* pgmq >= 0.26.0

## Installation
To install the package via Nuget, run the following command:

```bash
dotnet add package Npgmq
```

## Usage

Here is an example that uses Npgmq to create a queue and then send/read/archive a message:

```csharp
using Npgmq;

await using var connection = new NgpsqlConnection("<YOUR CONNECTION STRING HERE>");
var npgmq = new NpgmqClient(connection);

await npgmq.CreateQueueAsync("my_queue");

var msgId = await npgmq.SendAsync("my_queue", new MyMessageType
{
    Foo = "Test",
    Bar = 123
});
Console.WriteLine($"Sent message with id {msgId}");

var msg = await npgmq.ReadAsync<MyMessageType>("my_queue");
if (msg != null)
{
    Console.WriteLine($"Read message with id {msg.MsgId}: Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
    await npgmq.ArchiveAsync("my_queue", msg!.MsgId);
}
```

This example assumes you have defined a class called `MyMessageType` with the structure something like:

```csharp
public class MyMessageType
{
    public string Foo { get; set; } = null!;
    public int Bar { get; set; }
}
```

Alternatively you can send messages using an anonymous type and read them using an ExpandoObject, like this:
```csharp
using Npgmq;

await using var connection = new NgpsqlConnection("<YOUR CONNECTION STRING HERE>");
var npgmq = new NpgmqClient(connection);

await npgmq.CreateQueueAsync("my_queue");

var msgId = await npgmq.SendAsync("my_queue", new
{
    Foo = "Test",
    Bar = 123
});
Console.WriteLine($"Sent message with id {msgId}");

var msg = await npgmq.ReadAsync<ExpandoObject>("my_queue") as dynamic;
if (msg != null)
{
    Console.WriteLine($"Read message with id {msg.MsgId}: Foo = {msg.Message?.Foo}, Bar = {msg.Message?.Bar}");
    await npgmq.ArchiveAsync("my_queue", msg!.MsgId);
}
```