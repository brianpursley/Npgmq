namespace Npgmq.Test;

public class NpgmqHeadersTest
{
    [Fact]
    public void From_should_create_headers_dictionary_from_tuples()
    {
        // Act
        var headers = NpgmqHeaders.From(("key1", "value1"), ("key2", 123), ("key3", true));

        // Assert
        Assert.Equal(3, headers.Count);
        Assert.Equal("value1", headers["key1"]);
        Assert.Equal(123, headers["key2"]);
        Assert.Equal(true, headers["key3"]);
    }

    [Fact]
    public void From_should_create_headers_dictionary_from_single_key_value()
    {
        // Act
        var headers = NpgmqHeaders.From("key", "value");

        // Assert
        Assert.Single(headers);
        Assert.Equal("value", headers["key"]);
    }
}