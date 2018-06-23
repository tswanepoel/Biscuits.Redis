using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LIndex : BulkStringValueCommand
    {
        readonly byte[] _key;
        readonly long _index;

        public LIndex(Stream stream, byte[] key, long index)
            : base(stream, "LINDEX")
        {
            _key = key;
            _index = index;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_index.ToString(CultureInfo.InvariantCulture));
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_index.ToString(CultureInfo.InvariantCulture));
        }
    }
}
