namespace Biscuits.Redis.Resp
{
    public enum ReadState
    {
        Initial = 1,
        DataType = 2,
        Array = 3,
        Value = 4
    }
}
