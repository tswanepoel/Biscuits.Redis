using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
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

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_source);
            await writer.WriteBulkStringAsync(_destination);
        }
    }
}
