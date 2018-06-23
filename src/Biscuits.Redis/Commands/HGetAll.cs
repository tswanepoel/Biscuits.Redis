using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class HGetAll : ArrayOfBulkStringValueCommand
    {
        readonly byte[] _key;

        public HGetAll(Stream stream, byte[] key)
            : base(stream, "HGETALL")
        {
            _key = key;
        }
        
        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
        }
    }
}
