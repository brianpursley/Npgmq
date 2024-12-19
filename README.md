# Npgmq

A .NET client for [Postgres Message Queue](https://github.com/tembo-io/pgmq) (PGMQ).

[![Build](https://github.com/brianpursley/Npgmq/actions/workflows/build.yml/badge.svg)](https://github.com/brianpursley/Npgmq/actions/workflows/build.yml)
[![Nuget](https://img.shields.io/nuget/v/Npgmq)](https://www.nuget.org/packages/Npgmq/)
![License](https://img.shields.io/github/license/brianpursley/Npgmq)

## Compatibility

* pgmq >= 0.31.0

## Installation
To install the package via [Nuget](https://www.nuget.org/packages/Npgmq/), run the following command:

```shell
dotnet add package Npgmq
```

## Usage

Here is an example that uses Npgmq to create a queue and then send/read/archive a message:

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

This example assumes you have defined a class called `MyMessageType` with the structure something like:

```csharp
public class MyMessageType
{
    public string Foo { get; set; } = null!;
    public int Bar { get; set; }
}
```

You can send and read messages as JSON strings, like this:

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

You can pass your own `NpgsqlConnection` to the `NpgmqClient` constructor, like this:

```csharp
using var myConnection = new NpgsqlConnection("<YOUR CONNECTION STRING HERE>");
var npgmq = new NpgmqClient(myConnection);
```

## Database Connection

Npgmq uses Npgsql internally to connect to the database.

The connection string passed to the `NpgmqClient` constructor should be an [Npgsql connection string](https://www.npgsql.org/doc/connection-string-parameters.html).
