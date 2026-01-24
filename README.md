# Npgmq

A .NET client for [Postgres Message Queue](https://github.com/pgmq/pgmq) (PGMQ).

[![Build](https://github.com/brianpursley/Npgmq/actions/workflows/build.yml/badge.svg)](https://github.com/brianpursley/Npgmq/actions/workflows/build.yml)
[![Nuget](https://img.shields.io/nuget/v/Npgmq)](https://www.nuget.org/packages/Npgmq/)
![License](https://img.shields.io/github/license/brianpursley/Npgmq)

## Installation
To install the package via [Nuget](https://www.nuget.org/packages/Npgmq/), run the following command:

```shell
dotnet add package Npgmq
```

## Recommended Usage (TL;DR)

For most applications, use NpgsqlDataSource to create an NpgmqClient.

```csharp
await using var dataSource = NpgsqlDataSource.Create("<YOUR CONNECTION STRING HERE>");
var npgmq = new NpgmqClient(dataSource);
```

This provides the best connection pooling, performance, and safety.

You can also construct the client with a connection string or an existing NpgsqlConnection, but those options have tradeoffs. 
See [Database Connection](#database-connection) below.

## Basic Example

```csharp
using Npgmq;

var npgmq = new NpgmqClient("<YOUR CONNECTION STRING HERE>");

await npgmq.InitAsync();

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
    await npgmq.ArchiveAsync("my_queue", msg.MsgId);
}
```

```csharp
public class MyMessageType
{
    public string Foo { get; set; } = null!;
    public int Bar { get; set; }
}
```

## JSON Messages (Custom Serialization)

If you want full control over JSON serialization, you can send and receive messages as string:

```csharp   
var msgId = await npgmq.SendAsync("my_queue", "{\"foo\":\"Test\",\"bar\":123}");
Console.WriteLine($"Sent message with id {msgId}");

var msg = await npgmq.ReadAsync<string>("my_queue");
if (msg != null)
{
    Console.WriteLine($"Read message with id {msg.MsgId}: {msg.Message}");
    await npgmq.ArchiveAsync("my_queue", msg.MsgId);
}
```

Important:  
* When sending messages as string, the value must contain valid JSON.  
* NpgmqClient does not validate or escape string messages. Invalid JSON will cause the database call to fail.


## Database Connection

Npgmq uses [Npgsql](https://www.npgsql.org/) internally to connect to the database.

### Using NpgsqlDataSource (Recommended)

```csharp
await using var myDataSource = NpgsqlDataSource.Create("<YOUR CONNECTION STRING HERE>");
var npgmq = new NpgmqClient(myDataSource);
```

This is the preferred approach and provides optimal pooling and configuration.

### Using a Connection String

```csharp
var npgmq = new NpgmqClient("<YOUR CONNECTION STRING HERE>");
```

Connections are created as needed using the provided connection string.

### Using an Existing NpgsqlConnection

```csharp
await using var myConnection = new NpgsqlConnection("<YOUR CONNECTION STRING HERE>");
var npgmq = new NpgmqClient(myConnection);
```

When using this constructor:
* You are responsible for managing the connection lifecycle.
* NpgmqClient will open the connection if necessary, but will not close or dispose it.
* Concurrent usage is not supported. Ensure only one operation uses the connection at a time.

## Concurrency

NpgmqClient is safe for concurrent use when constructed with:
* an NpgsqlDataSource
* a connection string

When constructed with an existing NpgsqlConnection, concurrent operations are not supported.

## PGMQ Version Requirements

Npgmq is tested with pgmq versions 1.5.1 and higher.

Some features require minimum versions of the pgmq extension:
* PopAsync(queue, qty): Requires pgmq 1.7.0+
* SetVtBatchAsync: Requires pgmq 1.8.0+

You can check the installed version:
```csharp
var version = await npgmq.GetPgmqVersionAsync();
```

## License
MIT License. See [LICENSE](LICENSE) for details.
