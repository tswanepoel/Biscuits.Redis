using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class HDel : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly IEnumerable<byte[]> _fields;

        public HDel(Stream stream, byte[] key, IEnumerable<byte[]> fields)
            : base(stream, "HDEL")
        {
            _key = key;
            _fields = fields;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);

            foreach (byte[] field in _fields)
            {
                writer.WriteBulkString(field);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);

            foreach (byte[] field in _fields)
            {
                await writer.WriteBulkStringAsync(field);
            }
        }
    }
}
