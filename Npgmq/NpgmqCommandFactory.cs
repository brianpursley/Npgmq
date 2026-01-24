using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommandFactory
{
    private readonly NpgsqlDataSource? _dataSource;
    private readonly NpgsqlConnection? _connection;
    private readonly string? _connectionString;
    private readonly object _openLock = new();

    public NpgmqCommandFactory(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public NpgmqCommandFactory(NpgsqlConnection connection)
    {
        _connection = connection;
    }

    public NpgmqCommandFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<NpgmqCommand> CreateAsync(string commandText, CancellationToken cancellationToken = default)
    {
        var connection = await CreateConnectionFromDataSourceAsync(cancellationToken).ConfigureAwait(false) ??
                         await CreateConnectionFromConnectionStringAsync(cancellationToken).ConfigureAwait(false) ??
                         GetProvidedConnection() ??
                         throw new NpgmqException("No valid connection configuration provided.");

        return new NpgmqCommand(commandText, connection, _connection == null);
    }

    private NpgsqlConnection? GetProvidedConnection()
    {
        if (_connection == null)
        {
            return null;
        }

        EnsureProvidedConnectionIsOpen();
        return _connection;
    }

    private void EnsureProvidedConnectionIsOpen()
    {
        if (_connection == null || _connection.State == ConnectionState.Open)
        {
            return;
        }

        // Ideally, the connection would already be open, but if not, we will open it in a thread-safe way.
        lock (_openLock)
        {
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
            }
        }
    }

    private async Task<NpgsqlConnection?> CreateConnectionFromDataSourceAsync(CancellationToken cancellationToken)
    {
        if (_dataSource == null)
        {
            return null;
        }

        return await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<NpgsqlConnection?> CreateConnectionFromConnectionStringAsync(CancellationToken cancellationToken)
    {
        if (_connectionString == null)
        {
            return null;
        }

        var connection = new NpgsqlConnection(_connectionString!);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }
}