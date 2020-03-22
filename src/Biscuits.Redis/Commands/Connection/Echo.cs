using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Connection
{
    internal sealed class Echo : BulkStringValueCommand
    {
        private readonly byte[] _message;

        public Echo(Stream stream, byte[] message)
            : base(stream, "ECHO")
        {
            _message = message;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_message);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_message);
        }
    }
}
