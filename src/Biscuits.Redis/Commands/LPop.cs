using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LPop : BulkStringValueCommand
    {
        readonly byte[] _key;

        public LPop(Stream stream, byte[] key)
            : base(stream, "LPOP")
        {
            _key = key;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
        }
    }
}
