using System;
using System.Collections.Generic;

namespace DataTools.Sync.Core
{
    class BufferedQueue<T>
    {
        private readonly Func<BufferRange<T>, BufferRange<T>> _load;
        private readonly Queue<T> _buffer = new Queue<T>();
        private int _currentOffset;
        private readonly int _bufferMaxSize;
        private int _bufferRangeSize = 1000;
        private double _loadNextPageRatio = 0.4;
        private object _lock = new object();
        private bool _isEndOfBuffer;

        public BufferedQueue(Func<BufferRange<T>, BufferRange<T>> load, int size)
        {
            _load = load;
            _bufferMaxSize = size;
        }

        public bool TryDequeue(out T elem)
        {
            lock (_lock)
            {
                if (_buffer.Count > 0)
                {
                    elem = _buffer.Dequeue();
                    TryLoadPage();
                    return true;
                }

                TryLoadPage();
                if (_buffer.Count > 0)
                {
                    elem = _buffer.Dequeue();
                    return true;
                }

                elem = default(T);
                return false;
            }
        }

        private void TryLoadPage()
        {
            if (_isEndOfBuffer)
            {
                return;
            }

            if (_buffer.Count / _bufferMaxSize <= _loadNextPageRatio)
            {
                var result = _load(new BufferRange<T>()
                {
                    Offset = _currentOffset,
                    Size = _bufferRangeSize
                });

                _currentOffset += result.Result.Count;

                if (result.Result.Count < _bufferRangeSize)
                {
                    _isEndOfBuffer = true;
                }

                result.Result.ForEach(_buffer.Enqueue);
            }
        }
    }    

    class BufferRange<T>
    {
        public int Size { get; set; }
        public int Offset { get; set; }
        public List<T> Result { get; set; }
    }
}