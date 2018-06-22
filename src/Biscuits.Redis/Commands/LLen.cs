using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LLen : IntegerValueCommand
    {
        readonly byte[] _key;

        public LLen(Stream stream, byte[] key)
            : base(stream, "LLEN")
        {
            _key = key;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
        }
    }
}
