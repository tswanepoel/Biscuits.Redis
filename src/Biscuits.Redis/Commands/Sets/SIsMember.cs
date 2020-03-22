using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SIsMember : IntegerValueCommand
    {
        private readonly byte[] _key;
        private readonly byte[] _member;

        public SIsMember(Stream stream, byte[] key, byte[] member)
            : base(stream, "SISMEMBER")
        {
            _key = key;
            _member = member;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_member);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_member);
        }
    }
}
