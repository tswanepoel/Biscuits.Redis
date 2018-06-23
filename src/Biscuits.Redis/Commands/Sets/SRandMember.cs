using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SRandMember : ArrayOfBulkStringValueCommand
    {
        readonly byte[] _key;
        readonly long _count;

        public SRandMember(Stream stream, byte[] key, long count)
            : base(stream, "SRANDMEMBER")
        {
            _key = key;
            _count = count;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteInteger(_count);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteIntegerAsync(_count);
        }
    }
}
