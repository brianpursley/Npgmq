using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Npgmq.Test;

public sealed class PostgresFixture : IAsyncLifetime
{
    private const string DefaultConnectionString =
        "Host=localhost;Username=postgres;Password=postgres;Database=npgmq_test;";

    public string ConnectionString { get; private set; } = DefaultConnectionString;
    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public Task InitializeAsync()
    {
        var configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .AddUserSecrets<PostgresFixture>()
            .Build();

        ConnectionString = configuration.GetConnectionString("Test") ?? DefaultConnectionString;
        DataSource = NpgsqlDataSource.Create(ConnectionString);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await DataSource.DisposeAsync();
    }
}