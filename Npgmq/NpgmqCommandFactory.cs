using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommandFactory
{
    private readonly NpgsqlConnection? _connection;
    private readonly string? _connectionString;

    public NpgmqCommandFactory(NpgsqlConnection connection)
    {
        _connection = connection;
    }

    public NpgmqCommandFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<NpgmqCommand> CreateAsync(string commandText)
    {
        var connection = _connection ?? new NpgsqlConnection(_connectionString ?? throw new NpgmqException("No connection or connection string provided."));
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync().ConfigureAwait(false);
        }

        return new NpgmqCommand(commandText, connection, _connection == null);
    }
}