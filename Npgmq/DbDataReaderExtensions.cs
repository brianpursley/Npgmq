using System.Data.Common;

namespace Npgmq;

internal static class DbDataReaderExtensions
{
    /// <summary>
    /// Get the ordinal of a column by name, or -1 if the column does not exist.
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columnName">The column name.</param>
    /// <returns>The ordinal position of the column, or -1 if it does not exist in the reader.</returns>
    public static int TryGetOrdinal(this DbDataReader reader, string columnName)
    {
        try
        {
            return reader.GetOrdinal(columnName);
        }
        catch (IndexOutOfRangeException)
        {
            return -1;
        }
    }
}