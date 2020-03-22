using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SRem : IntegerValueCommand
    {
        private readonly byte[] _key;
        private readonly IEnumerable<byte[]> _members;

        public SRem(Stream stream, byte[] key, IEnumerable<byte[]> members)
            : base(stream, "SREM")
        {
            _key = key;
            _members = members;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);

            foreach (var member in _members)
            {
                writer.WriteBulkString(member);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);

            foreach (var member in _members)
            {
                await writer.WriteBulkStringAsync(member);
            }
        }
    }
}
