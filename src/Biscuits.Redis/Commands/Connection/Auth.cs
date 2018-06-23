using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Connection
{
    internal sealed class Auth : SimpleStringValueCommand
    {
        readonly byte[] _password;

        public Auth(Stream stream, byte[] password)
            : base(stream, "AUTH")
        {
            _password = password;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_password);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_password);
        }
    }
}
