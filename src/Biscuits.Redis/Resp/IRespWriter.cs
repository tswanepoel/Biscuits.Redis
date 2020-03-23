namespace Biscuits.Redis.Resp
{
    public interface IRespWriter
    {
        WriteState WriteState { get; }
        void WriteStartArray();
        void WriteEndArray();
        void WriteNullArray();
        void WriteSimpleStringUnsafe(string value);
        void WriteErrorUnsafe(string value);
        void WriteInteger(long value);
        void WriteBulkString(string value);
        void WriteBulkString(byte[] bytes);
        void Flush();
    }
}
