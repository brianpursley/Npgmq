using System.Data;
using Npgsql;

namespace Npgmq;

internal class NpgmqCommand(string commandText, NpgsqlConnection connection, bool disposeConnection)
    : NpgsqlCommand(commandText, connection)
{
    protected override void Dispose(bool disposing)
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

        base.Dispose(disposing);
    }
}