using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SMembers : ArrayOfBulkStringValueCommand
    {
        readonly byte[] _key;

        public SMembers(Stream stream, byte[] key)
            : base(stream, "SMEMBERS")
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
