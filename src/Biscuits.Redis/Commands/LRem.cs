using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LRem : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly long _count;
        readonly byte[] _value;

        public LRem(Stream stream, byte[] key, long count, byte[] value)
            : base(stream, "LREM")
        {
            _key = key;
            _count = count;
            _value = value;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_count.ToString(CultureInfo.InvariantCulture));
            writer.WriteBulkString(_value);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_count.ToString(CultureInfo.InvariantCulture));
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
