using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Npgmq;

/// <summary>
/// Extension methods for registering Npgmq services in an <see cref="IServiceCollection"/>.
/// </summary>
public static class NpgmqServiceCollectionExtensions
{
    extension(IServiceCollection serviceCollection)
    {
        /// <summary>
        /// Add NpgmqClient to the service collection.
        /// </summary>
        /// <param name="lifetime">The service lifetime.</param>
        /// <param name="serviceKey">Key to register the service.</param>
        /// <returns></returns>
        /// <remarks>
        /// This method requires that an NpgsqlDataSource is already registered in the service collection.
        /// </remarks>
        public IServiceCollection AddNpgmqClient(ServiceLifetime lifetime = ServiceLifetime.Singleton,
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
        /// <param name="connectionString">The connection string to use for the NpgsqlDataSource.</param>
        /// <param name="lifetime">The lifetime of the NpgmqClient.</param>
        /// <param name="serviceKey">Key to register the service.</param>
        /// <returns></returns>
        /// <remarks>
        /// This method registers an NpgsqlDataSource in the service collection using the provided connection string.
        /// </remarks>
        public IServiceCollection AddNpgmqClient(string connectionString,
            ServiceLifetime lifetime = ServiceLifetime.Singleton,
            object? serviceKey = null)
        {
            if (serviceKey is not null)
            {
                serviceCollection.TryAdd(
                    new ServiceDescriptor(
                        typeof(NpgsqlDataSource),
                        serviceKey,
                        (_, _) => NpgsqlDataSource.Create(connectionString),
                        ServiceLifetime.Singleton));
            }
            else
            {
                serviceCollection.TryAdd(
                    new ServiceDescriptor(
                        typeof(NpgsqlDataSource),
                        _ => NpgsqlDataSource.Create(connectionString),
                        ServiceLifetime.Singleton));
            }

            serviceCollection.AddNpgmqClient(lifetime, serviceKey);

            return serviceCollection;
        }
    }
}