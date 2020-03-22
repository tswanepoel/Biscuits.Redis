using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Connection
{
    internal sealed class Select : SimpleStringValueCommand
    {
        private readonly int _index;

        public Select(Stream stream, int index)
            : base(stream, "SELECT")
        {
            _index = index;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_index.ToString(CultureInfo.InvariantCulture));
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_index.ToString(CultureInfo.InvariantCulture));
        }
    }
}
