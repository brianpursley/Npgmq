using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Npgmq;

/// <summary>
/// Extension methods for registering Npgmq services in an <see cref="IServiceCollection"/>.
/// </summary>
public static class NpgmqServiceCollectionExtensions
{
    /// <summary>
    /// Add NpgmqClient to the service collection.
    /// </summary>
    /// <param name="serviceCollection">The service collection.</param>
    /// <param name="lifetime">The service lifetime.</param>
    /// <param name="serviceKey">Key to register the service.</param>
    /// <returns>The service collection.</returns>
    /// <remarks>
    /// This method requires that an NpgsqlDataSource is already registered in the service collection.
    /// </remarks>
    public static IServiceCollection AddNpgmqClient(
        this IServiceCollection serviceCollection,
        ServiceLifetime lifetime = ServiceLifetime.Singleton,
        object? serviceKey = null)
    {
        if (serviceKey is not null)
        {
            serviceCollection.TryAdd(
                new ServiceDescriptor(
                    typeof(NpgmqClient),
                    serviceKey,
                    (sp, key) => new NpgmqClient(sp.GetRequiredKeyedService<NpgsqlDataSource>(key)),
                    lifetime));

            serviceCollection.TryAdd(
                new ServiceDescriptor(
                    typeof(INpgmqClient),
                    serviceKey,
                    (sp, key) => sp.GetRequiredKeyedService<NpgmqClient>(key),
                    lifetime));
        }
        else
        {
            serviceCollection.TryAdd(
                new ServiceDescriptor(
                    typeof(NpgmqClient),
                    sp => new NpgmqClient(sp.GetRequiredService<NpgsqlDataSource>()),
                    lifetime));

            serviceCollection.TryAdd(
                new ServiceDescriptor(
                    typeof(INpgmqClient),
                    sp => sp.GetRequiredService<NpgmqClient>(),
                    lifetime));
        }

        return serviceCollection;
    }

    /// <summary>
    /// Add NpgmqClient to the service collection using the provided connection string.
    /// </summary>
    /// <param name="serviceCollection">The service collection.</param>
    /// <param name="connectionString">The connection string to use for the NpgsqlDataSource.</param>
    /// <param name="lifetime">The lifetime of the NpgmqClient.</param>
    /// <param name="serviceKey">Key to register the service.</param>
    /// <returns>The service collection.</returns>
    /// <remarks>
    /// This method registers an NpgsqlDataSource as a singleton in the service collection using the provided connection string and service key.
    /// </remarks>
    public static IServiceCollection AddNpgmqClient(
        this IServiceCollection serviceCollection,
        string connectionString,
        ServiceLifetime lifetime = ServiceLifetime.Singleton,
        object? serviceKey = null)
    {
        serviceCollection.AddNpgsqlDataSource(connectionString, serviceKey: serviceKey);
        serviceCollection.AddNpgmqClient(lifetime, serviceKey);
        return serviceCollection;
    }
}