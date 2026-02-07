using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    private int _connectionDisposed;

    public override async ValueTask DisposeAsync()
    {
        // Dispose base command first to allow proper cleanup/unprepare
        await base.DisposeAsync().ConfigureAwait(false);

        // Then dispose the owned connection if needed (thread-safe check)
        if (disposeConnection && Connection != null && Interlocked.CompareExchange(ref _connectionDisposed, 1, 0) == 0)
        {
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void Dispose(bool disposing)
    {
        // Dispose base command first to allow proper cleanup/unprepare
        base.Dispose(disposing);

        // Then dispose the owned connection if needed (thread-safe check)
        if (disposing && disposeConnection && Connection != null && Interlocked.CompareExchange(ref _connectionDisposed, 1, 0) == 0)
        {
            Connection.Dispose();
        }
    }
}