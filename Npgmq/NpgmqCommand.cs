using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    private bool _connectionDisposed;

    public override async ValueTask DisposeAsync()
    {
        // Dispose base command first to allow proper cleanup/unprepare
        await base.DisposeAsync().ConfigureAwait(false);

        // Then dispose the owned connection if needed
        if (disposeConnection && Connection != null && !_connectionDisposed)
        {
            _connectionDisposed = true;
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void Dispose(bool disposing)
    {
        // Dispose base command first to allow proper cleanup/unprepare
        base.Dispose(disposing);

        // Then dispose the owned connection if needed
        if (disposing && disposeConnection && Connection != null && !_connectionDisposed)
        {
            _connectionDisposed = true;
            Connection.Dispose();
        }
    }
}