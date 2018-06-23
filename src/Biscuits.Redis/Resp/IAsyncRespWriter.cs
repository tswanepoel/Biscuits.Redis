using System.Threading.Tasks;

namespace Biscuits.Redis.Resp
{
    public interface IAsyncRespWriter
    {
        WriteState WriteState { get; }
        Task WriteStartArrayAsync();
        Task WriteEndArrayAsync();
        Task WriteNullArrayAsync();
        Task WriteSimpleStringUnsafeAsync(string value);
        Task WriteErrorUnsafeAsync(string value);
        Task WriteIntegerAsync(long value);
        Task WriteBulkStringAsync(string value);
        Task WriteBulkStringAsync(byte[] bytes);
        Task FlushAsync();
        void Close();
    }
}
