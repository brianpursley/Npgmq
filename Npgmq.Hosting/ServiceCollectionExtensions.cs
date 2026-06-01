using Microsoft.Extensions.DependencyInjection;

namespace Npgmq;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddNpgmqHosting(
        this IServiceCollection services,
        Action<NpgmqHostingOptions> configure
    )
    {
        services.AddHostedService<NpgmqBackgroundService>();

        services.Configure(configure);

        return services;
    }
}