using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LSet : SimpleStringValueCommand
    {
        readonly byte[] _key;
        readonly long _index;
        readonly byte[] _value;

        public LSet(Stream stream, byte[] key, long index, byte[] value)
            : base(stream, "LSET")
        {
            _key = key;
            _index = index;
            _value = value;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_index.ToString(CultureInfo.InvariantCulture));
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
