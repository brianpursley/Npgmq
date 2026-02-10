using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    // Track whether the connection has been disposed to avoid double-disposal.
    private int _connectionHasBeenDisposed;

    public override async ValueTask DisposeAsync()
    {
        try
        {
            if (disposeConnection && Connection != null && Interlocked.CompareExchange(ref _connectionHasBeenDisposed, 1, 0) == 0)
            {
                await Connection.DisposeAsync().ConfigureAwait(false);
                Connection = null;
            }
        }
        finally
        {
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void Dispose(bool disposing)
    {
        try
        {
            if (disposeConnection && Connection != null && Interlocked.CompareExchange(ref _connectionHasBeenDisposed, 1, 0) == 0)
            {
                Connection.Dispose();
                Connection = null;
            }
        }
        finally
        {
            base.Dispose(disposing);
        }
    }
}