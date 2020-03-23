using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Hashes
{
    internal sealed class HMGet : ArrayOfBulkStringValueCommand
    {
        private readonly byte[] _key;
        private readonly IEnumerable<byte[]> _fields;

        public HMGet(Stream stream, byte[] key, IEnumerable<byte[]> fields)
            : base(stream, "HMGET")
        {
            _key = key;
            _fields = fields;
        }
        
        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);

            foreach (var field in _fields)
            {
                writer.WriteBulkString(field);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);

            foreach (var field in _fields)
            {
                await writer.WriteBulkStringAsync(field);
            }
        }
    }
}
