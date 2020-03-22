using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Hashes
{
    internal sealed class HKeys : ArrayOfBulkStringValueCommand
    {
        private readonly byte[] _key;

        public HKeys(Stream stream, byte[] key)
            : base(stream, "HKEYS")
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
