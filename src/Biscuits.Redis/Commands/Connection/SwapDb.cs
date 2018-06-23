using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Connection
{
    internal sealed class SwapDb : SimpleStringValueCommand
    {
        readonly int _index1;
        readonly int _index2;

        public SwapDb(Stream stream, int index1, int index2)
            : base(stream, "SWAPDB")
        {
            _index1 = index1;
            _index2 = index2;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_index1.ToString(CultureInfo.InvariantCulture));
            writer.WriteBulkString(_index2.ToString(CultureInfo.InvariantCulture));
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_index1.ToString(CultureInfo.InvariantCulture));
            await writer.WriteBulkStringAsync(_index2.ToString(CultureInfo.InvariantCulture));
        }
    }
}
