using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LIndex : BulkStringValueCommand
    {
        private readonly byte[] _key;
        private readonly long _index;

        public LIndex(Stream stream, byte[] key, long index)
            : base(stream, "LINDEX")
        {
            _key = key;
            _index = index;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_index.ToString(CultureInfo.InvariantCulture));
        }
    }
}
