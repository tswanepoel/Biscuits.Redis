﻿using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LInsertBefore : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly byte[] _before;
        readonly byte[] _value;

        public LInsertBefore(Stream stream, byte[] key, byte[] before, byte[] value)
            : base(stream, "LINSERT")
        {
            _key = key;
            _before = before;
            _value = value;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync("BEFORE");
            await writer.WriteBulkStringAsync(_before);
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
