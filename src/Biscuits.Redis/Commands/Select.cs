using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class Select : SimpleStringValueCommand
    {
        readonly int _index;

        public Select(Stream stream, int index)
            : base(stream, "SELECT")
        {
            _index = index;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_index.ToString(CultureInfo.InvariantCulture));
        }
    }
}
