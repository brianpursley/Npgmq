using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Npgmq.Test;

[Collection("Npgmq")]
public sealed class NpgmqServiceCollectionExtensionsTest(PostgresFixture postgresFixture) : IClassFixture<PostgresFixture>
{
    private static ServiceProvider CreateServiceProvider(IServiceCollection services)
        => services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateScopes = true,
            ValidateOnBuild = true
        });

    [Theory]
    [InlineData(ServiceLifetime.Singleton, true)]
    [InlineData(ServiceLifetime.Scoped, true)]
    [InlineData(ServiceLifetime.Transient, false)]
    public void AddNpgmqClient_Unkeyed_Resolves_And_INpgmqClient_Is_Alias(ServiceLifetime lifetime, bool shouldBeSame)
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<NpgsqlDataSource>(_ => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient(lifetime: lifetime, serviceKey: null);

        // Assert
        using var root = CreateServiceProvider(services);
        using var scope = root.CreateScope();
        var client1 = scope.ServiceProvider.GetRequiredService<NpgmqClient>();
        var client2 = scope.ServiceProvider.GetRequiredService<INpgmqClient>();
        if (shouldBeSame)
        {
            Assert.Same(client1, client2);
        }
        else
        {
            Assert.NotSame(client1, client2);
        }
    }

    [Fact]
    public void AddNpgmqClient_Unkeyed_Singleton_Lifetime_Works()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<NpgsqlDataSource>(_ => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient();

        // Assert
        using var root = CreateServiceProvider(services);
        var client1 = root.GetRequiredService<NpgmqClient>();
        var client2 = root.GetRequiredService<NpgmqClient>();
        Assert.Same(client1, client2);
        using var scope1 = root.CreateScope();
        using var scope2 = root.CreateScope();
        Assert.Same(scope1.ServiceProvider.GetRequiredService<NpgmqClient>(),
                    scope2.ServiceProvider.GetRequiredService<NpgmqClient>());
    }

    [Fact]
    public void AddNpgmqClient_Unkeyed_Scoped_Lifetime_Works()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<NpgsqlDataSource>(_ => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient(ServiceLifetime.Scoped);

        // Assert
        using var root = CreateServiceProvider(services);
        using var scope1 = root.CreateScope();
        using var scope2 = root.CreateScope();
        var client1 = scope1.ServiceProvider.GetRequiredService<NpgmqClient>();
        var client2 = scope1.ServiceProvider.GetRequiredService<NpgmqClient>();
        Assert.Same(client1, client2);
        var client3 = scope2.ServiceProvider.GetRequiredService<NpgmqClient>();
        Assert.NotSame(client1, client3);
    }

    [Fact]
    public void AddNpgmqClient_Unkeyed_Transient_Lifetime_Works()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<NpgsqlDataSource>(_ => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient(ServiceLifetime.Transient);

        // Assert
        using var root = CreateServiceProvider(services);
        var client1 = root.GetRequiredService<NpgmqClient>();
        var client2 = root.GetRequiredService<NpgmqClient>();
        Assert.NotSame(client1, client2);
    }

    [Theory]
    [InlineData(ServiceLifetime.Singleton, true)]
    [InlineData(ServiceLifetime.Scoped, true)]
    [InlineData(ServiceLifetime.Transient, false)]
    public void AddNpgmqClient_Keyed_Resolves_And_INpgmqClient_Is_Alias(ServiceLifetime lifetime, bool shouldBeSame)
    {
        // Arrange
        const string key = "a";
        var services = new ServiceCollection();
        services.AddKeyedSingleton<NpgsqlDataSource>(key, (_, _) => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient(lifetime: lifetime, serviceKey: key);

        // Assert
        using var root = CreateServiceProvider(services);
        using var scope = root.CreateScope();
        var client1 = scope.ServiceProvider.GetRequiredKeyedService<NpgmqClient>(key);
        var client2 = scope.ServiceProvider.GetRequiredKeyedService<INpgmqClient>(key);
        if (shouldBeSame)
        {
            Assert.Same(client1, client2);
        }
        else
        {
            Assert.NotSame(client1, client2);
        }
    }

    [Fact]
    public void AddNpgmqClient_Keyed_Allows_Multiple_Keys()
    {
        // Arrange
        const string keyA = "a";
        const string keyB = "b";
        var services = new ServiceCollection();
        services.AddKeyedSingleton<NpgsqlDataSource>(keyA, (_, _) => postgresFixture.DataSource);
        services.AddKeyedSingleton<NpgsqlDataSource>(keyB, (_, _) => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient(serviceKey: keyA);
        services.AddNpgmqClient(serviceKey: keyB);

        // Assert
        using var root = CreateServiceProvider(services);
        var client1 = root.GetRequiredKeyedService<INpgmqClient>(keyA);
        Assert.NotNull(client1);
        var client2 = root.GetRequiredKeyedService<INpgmqClient>(keyB);
        Assert.NotNull(client2);
        Assert.NotSame(client1, client2);
    }

    [Fact]
    public void AddNpgmqClient_With_ConnectionString_Registers_NpgsqlDataSource_As_Singleton_Unkeyed()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNpgmqClient(postgresFixture.ConnectionString, lifetime: ServiceLifetime.Transient, serviceKey: null);

        // Assert
        var dsDescriptor = services.Single(sd => sd.ServiceType == typeof(NpgsqlDataSource) && sd.ServiceKey is null);
        Assert.Equal(ServiceLifetime.Singleton, dsDescriptor.Lifetime);
    }

    [Fact]
    public void AddNpgmqClient_With_ConnectionString_Registers_NpgsqlDataSource_As_Singleton_Keyed()
    {
        // Arrange
        const string key = "a";
        var services = new ServiceCollection();

        // Act
        services.AddNpgmqClient(postgresFixture.ConnectionString, lifetime: ServiceLifetime.Transient, serviceKey: key);

        // Assert
        var dsDescriptor = services.Single(sd => sd.ServiceType == typeof(NpgsqlDataSource) && Equals(sd.ServiceKey, key));
        Assert.Equal(ServiceLifetime.Singleton, dsDescriptor.Lifetime);
    }

    [Fact]
    public void AddNpgmqClient_DoesNot_Add_Duplicate_Unkeyed_Descriptors()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<NpgsqlDataSource>(_ => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient();
        services.AddNpgmqClient();

        // Assert
        Assert.Equal(1, services.Count(sd => sd.ServiceType == typeof(NpgmqClient) && sd.ServiceKey is null));
        Assert.Equal(1, services.Count(sd => sd.ServiceType == typeof(INpgmqClient) && sd.ServiceKey is null));
    }

    [Fact]
    public void AddNpgmqClient_DoesNot_Add_Duplicate_Keyed_Descriptors_For_Same_Key()
    {
        // Arrange
        const string key = "a";
        var services = new ServiceCollection();
        services.AddKeyedSingleton<NpgsqlDataSource>(key, (_, _) => postgresFixture.DataSource);

        // Act
        services.AddNpgmqClient(serviceKey: key);
        services.AddNpgmqClient(serviceKey: key);

        // Assert
        Assert.Equal(1, services.Count(sd => sd.ServiceType == typeof(NpgmqClient) && Equals(sd.ServiceKey, key)));
        Assert.Equal(1, services.Count(sd => sd.ServiceType == typeof(INpgmqClient) && Equals(sd.ServiceKey, key)));
    }
    
    [Fact]
    public async Task AddNpgmqClient_resolves_to_a_functional_client()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNpgsqlDataSource(postgresFixture.ConnectionString);
        services.AddNpgmqClient();
        var testQueueName = $"test_{Guid.NewGuid():N}";
        
        // Act
        await using var provider = CreateServiceProvider(services);
        var client = provider.GetRequiredService<INpgmqClient>();

        // Assert
        Assert.NotNull(client);
        await client.CreateQueueAsync(testQueueName);
        Assert.True(await client.QueueExistsAsync(testQueueName));
        Assert.True(await client.DropQueueAsync(testQueueName));
    }
    
    [Fact]
    public async Task AddNpgmqClient_created_with_connection_string_resolves_to_a_functional_client()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNpgmqClient(postgresFixture.ConnectionString);
        var testQueueName = $"test_{Guid.NewGuid():N}";
        
        // Act
        await using var provider = CreateServiceProvider(services);
        var client = provider.GetRequiredService<INpgmqClient>();

        // Assert
        Assert.NotNull(client);
        await client.CreateQueueAsync(testQueueName);
        Assert.True(await client.QueueExistsAsync(testQueueName));
        Assert.True(await client.DropQueueAsync(testQueueName));;
    }
}