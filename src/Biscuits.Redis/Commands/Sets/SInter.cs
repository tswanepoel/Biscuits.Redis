using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SInter : ArrayOfBulkStringValueCommand
    {
        readonly IEnumerable<byte[]> _keys;

        public SInter(Stream stream, IEnumerable<byte[]> keys)
            : base(stream, "SINTER")
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
