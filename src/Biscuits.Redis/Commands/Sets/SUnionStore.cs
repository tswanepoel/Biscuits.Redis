using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SUnionStore : ArrayOfBulkStringValueCommand
    {
        readonly byte[] _destination;
        readonly IEnumerable<byte[]> _keys;

        public SUnionStore(Stream stream, byte[] destination, IEnumerable<byte[]> keys)
            : base(stream, "SUNIONSTORE")
        {
            _destination = destination;
            _keys = keys;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_destination);

            foreach (byte[] key in _keys)
            {
                writer.WriteBulkString(key);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
           await  writer.WriteBulkStringAsync(_destination);

            foreach (byte[] key in _keys)
            {
                await writer.WriteBulkStringAsync(key);
            }
        }
    }
}
