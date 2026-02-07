using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    public override async ValueTask DisposeAsync()
    {
        try
        {
            if (disposeConnection && Connection != null)
            {
                if (Connection.State == ConnectionState.Open)
                {
                    await Connection.CloseAsync().ConfigureAwait(false);
                }

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
            if (disposing && disposeConnection && Connection != null)
            {
                if (Connection.State == ConnectionState.Open)
                {
                    Connection.Close();
                }

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