namespace Npgmq;

/// <summary>
/// An exception thrown by Npgmq.
/// </summary>
public class NpgmqException : Exception
{
    /// <summary>
    /// Create a new <see cref="NpgmqException"/>.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public NpgmqException(string message) : base(message)
    {
    }

    /// <summary>
    /// Create a new <see cref="NpgmqException"/>.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public NpgmqException(string message, Exception innerException) : base(message, innerException)
    {
    }
}