using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SDiffStore : IntegerValueCommand
    {
        private readonly IEnumerable<byte[]> _keys;

        public SDiffStore(Stream stream, IEnumerable<byte[]> keys)
            : base(stream, "SDIFFSTORE")
        {
            _keys = keys;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            foreach (var key in _keys)
            {
                writer.WriteBulkString(key);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            foreach (var key in _keys)
            {
                await writer.WriteBulkStringAsync(key);
            }
        }
    }
}
