using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Lists
{
    internal sealed class RPopLPush : BulkStringValueCommand
    {
        readonly byte[] _source;
        readonly byte[] _destination;

        public RPopLPush(Stream stream, byte[] source, byte[] destination)
            : base(stream, "RPOPLPUSH")
        {
            _source = source;
            _destination = destination;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_source);
            writer.WriteBulkString(_destination);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_source);
            await writer.WriteBulkStringAsync(_destination);
        }
    }
}
