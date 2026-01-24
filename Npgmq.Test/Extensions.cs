namespace Npgmq.Test;

static class Extensions
{
    public static DateTimeOffset TruncateToMicroseconds(this DateTimeOffset dto)
    {
        var ticks = dto.Ticks - (dto.Ticks % 10);
        return new DateTimeOffset(ticks, dto.Offset);
    }

    public static DateTimeOffset ToDateTimeOffset(this DateTime dt)
    {
        return dt.Kind == DateTimeKind.Utc
            ? new DateTimeOffset(dt)
            : new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc));
    }
}