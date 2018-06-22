using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LPushX : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly byte[] _value;

        public LPushX(Stream stream, byte[] key, byte[] value)
            : base(stream, "LPUSHX")
        {
            _key = key;
            _value = value;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
