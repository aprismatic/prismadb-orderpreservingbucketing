using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace PrismaDB.OrderPreservingBucketing
{
    public class ConcurrentSortedList
    {
        private object lockObj;
        private List<UInt64> container;

        public ConcurrentSortedList()
        {
            lockObj = new object();
            container = new List<UInt64>();
        }

        public int Count
        {
            get
            {
                lock (lockObj)
                {
                    return container.Count;
                }
            }
        }

        public void Add(UInt64 item)
        {
            lock (lockObj)
            {
                if (container.Count == 0)
                {
                    container.Add(item);
                }
                else
                {
                    var (a, b) = BinarySearchNoLock(item);
                    if (a != b)
                    {
                        container.Insert(b, item);
                    }
                }
            }
        }

        public bool Contains(UInt64 item)
        {
            var (a, b) = BinarySearch(item);
            return a == b;
        }

        public void RemoveAt(int index)
        {
            lock (lockObj)
            {
                container.RemoveAt(index);
            }
        }

        public UInt64 this[int i]
        {
            get {
                lock (lockObj)
                {
                    return container[i];
                }
            }
        }

        public (int, int) BinarySearch(UInt64 item)
        {
            lock (lockObj)
            {
                return BinarySearchNoLock(item);
            }
        }

        private (int, int) BinarySearchNoLock(UInt64 item)
        {
            var begin = 0;
            var end = container.Count - 1;

            if (-1 == end)
                return (-1, 0);

            if (item < container[0])
                return (-1, 0);

            if (item > container[end])
                return (end, end + 1);

            while (end > begin)
            {
                var index = (begin + end) / 2;
                var el = container[index];

                if (el > item)
                    end = index;
                else if (el < item)
                    begin = index + 1;
                else
                    return (index, index);
            }

            return container[end] == item ? (begin, end) : (begin - 1, end);
        }
    }
}
