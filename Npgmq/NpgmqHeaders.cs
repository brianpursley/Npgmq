namespace Npgmq;

public static class NpgmqHeaders
{
    public const string XPgmqGroup = "x-pgmq-group";

    public static Dictionary<string, object> From(params ValueTuple<string, object>[] tuples)
    {
        var headers = new Dictionary<string, object>();
        foreach (var tuple in tuples)
        {
            headers[tuple.Item1] = tuple.Item2;
        }
        return headers;
    }

    public static Dictionary<string, object> From(string key, object value) => From((key, value));
}