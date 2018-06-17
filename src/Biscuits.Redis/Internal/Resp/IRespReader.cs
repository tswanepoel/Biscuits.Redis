namespace Biscuits.Redis.Resp
{
    internal interface IRespReader
    {
        ReadState ReadState { get; }
        RespDataType ReadDataType();
        bool TryReadStartArray(out long length);
        void ReadEndArray();
        string ReadSimpleStringValue();
        string ReadErrorValue();
        byte[] ReadBulkStringValue();
        long ReadIntegerValue();
        void Close();
    }
}
