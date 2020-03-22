using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SMove : ArrayOfBulkStringValueCommand
    {
        private readonly byte[] _source;
        private readonly byte[] _destination;
        private readonly byte[] _member;

        public SMove(Stream stream, byte[] source, byte[] destination, byte[] member)
            : base(stream, "SMOVE")
        {
            _source = source;
            _destination = destination;
            _member = member;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_source);
            writer.WriteBulkString(_destination);
            writer.WriteBulkString(_member);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_source);
            await writer.WriteBulkStringAsync(_destination);
            await writer.WriteBulkStringAsync(_member);
        }
    }
}
