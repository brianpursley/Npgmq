using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    public override async ValueTask DisposeAsync()
    {
        if (disposeConnection && Connection != null)
        {
            if (Connection.State == ConnectionState.Open)
            {
                await Connection.CloseAsync().ConfigureAwait(false);
            }
            
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
