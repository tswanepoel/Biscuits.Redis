using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class Echo : BulkStringValueCommand
    {
        readonly byte[] _message;

        public Echo(Stream stream, byte[] message)
            : base(stream, "ECHO")
        {
            _message = message;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_message);
        }
    }
}
