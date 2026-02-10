using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    // Track whether the connection has been disposed to avoid double-disposal.
    private int _connectionHasBeenDisposed;

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync().ConfigureAwait(false);

        if (disposeConnection && Connection != null && Interlocked.CompareExchange(ref _connectionHasBeenDisposed, 1, 0) == 0)
        {
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing && disposeConnection && Connection != null && Interlocked.CompareExchange(ref _connectionHasBeenDisposed, 1, 0) == 0)
        {
            Connection.Dispose();
        }
    }
}