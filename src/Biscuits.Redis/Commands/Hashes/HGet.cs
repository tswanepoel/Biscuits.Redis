using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Hashes
{
    internal sealed class HGet : BulkStringValueCommand
    {
        private readonly byte[] _key;
        private readonly byte[] _field;

        public HGet(Stream stream, byte[] key, byte[] field)
            : base(stream, "HGET")
        {
            _key = key;
            _field = field;
        }
        
        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_field);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_field);
        }
    }
}
