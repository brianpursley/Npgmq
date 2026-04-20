namespace Npgmq;

/// <summary>
/// Helper class for creating message headers in a more convenient way.
/// </summary>
public static class NpgmqHeaders
{
    /// <summary>
    /// Creates a headers dictionary from an array of key-value tuples.
    /// </summary>
    /// <param name="tuples">The key-value pairs to include in the headers.</param>
    /// <returns>A dictionary representing the headers.</returns>
    public static Dictionary<string, object> From(params ValueTuple<string, object>[] tuples)
    {
        var headers = new Dictionary<string, object>();
        foreach (var tuple in tuples)
        {
            headers[tuple.Item1] = tuple.Item2;
        }
        return headers;
    }

    /// <summary>
    /// Creates a headers dictionary from a single key-value pair.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <param name="value">The header value.</param>
    /// <returns>>A dictionary representing the headers.</returns>
    public static Dictionary<string, object> From(string key, object value) => From((key, value));
}