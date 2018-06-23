using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SDiff : ArrayOfBulkStringValueCommand
    {
        readonly IEnumerable<byte[]> _keys;

        public SDiff(Stream stream, IEnumerable<byte[]> keys)
            : base(stream, "SDIFF")
        {
            _keys = keys;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            foreach (byte[] key in _keys)
            {
                writer.WriteBulkString(key);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            foreach (byte[] key in _keys)
            {
                await writer.WriteBulkStringAsync(key);
            }
        }
    }
}
