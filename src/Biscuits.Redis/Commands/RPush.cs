﻿using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class RPush : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly IEnumerable<byte[]> _values;

        public RPush(Stream stream, byte[] key, IEnumerable<byte[]> values)
            : base(stream, "RPUSH")
        {
            _key = key;
            _values = values;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);

            foreach (var value in _values)
            {
                await writer.WriteBulkStringAsync(value);
            }
        }
    }
}
