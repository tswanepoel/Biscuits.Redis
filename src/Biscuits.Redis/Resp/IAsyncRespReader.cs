using System.Threading.Tasks;

namespace Biscuits.Redis.Resp
{
    public interface IAsyncRespReader
    {
        ReadState ReadState { get; }
        Task<RespDataType> ReadDataTypeAsync();
        Task<(bool success, long length)> TryReadStartArrayAsync();
        Task ReadEndArrayAsync();
        Task<string> ReadSimpleStringValueAsync();
        Task<string> ReadErrorValueAsync();
        Task<byte[]> ReadBulkStringValueAsync();
        Task<long> ReadIntegerValueAsync();
    }
}
