using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Connection
{
    internal sealed class Ping : SimpleStringValueCommand
    {
        readonly byte[] _message;

        public Ping(Stream stream)
            : this(stream, null)
        {
        }

        public Ping(Stream stream, byte[] message)
            : base(stream, "PING")
        {
            _message = message;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            if (_message != null)
            {
                writer.WriteBulkString(_message);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            if (_message != null)
            {
                await writer.WriteBulkStringAsync(_message);
            }
        }
    }
}
