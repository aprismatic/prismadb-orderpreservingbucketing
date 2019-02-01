using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace PrismaDB.OrderPreservingBucketing
{
    public class OpfInteger
    {
        private readonly UInt64 _width;

        private ConcurrentSortedList _bucketNos; // existing bucket numbers
        private ConcurrentDictionary<UInt64, Int64> _bucketIds; // bucket number -> bucket id
        private ConcurrentDictionary<Int64, byte> _bucketIdList; // existing bucket Ids

        private object lockObj;

        public OpfInteger(int width)
        {
            if (width < 3) throw new ApplicationException("Bucket width should be >= 3");
            _width = (UInt64)width;
            _bucketNos = new ConcurrentSortedList();
            _bucketIds = new ConcurrentDictionary<UInt64, Int64>();
            _bucketIdList = new ConcurrentDictionary<Int64, byte>();
            lockObj = new Object();
        }

        /// <summary>
        /// Returns the bucket ID for <c>value</c>.
        /// </summary>
        public Int64 GetBucketId(Int64 value)
        {
            var bucketNo = _GetBucketNumber(value);

            Int64 res;

            lock (lockObj)
            {
                if (_bucketIds.ContainsKey(bucketNo))
                    res = _bucketIds[bucketNo];
                else
                    res = _GenerateBucketId(bucketNo);
            }

            return res;
        }

        /// <summary>
        /// Returns the bucket value range for min &lt= <c>value</c> &lt max.
        /// </summary>
        public (Int64, Int64) GetBucketRange(Int64 value)
        {
            var bottom = uiabs(Int64.MinValue);
            var bucketNumber = _GetBucketNumber(value);

            var min = uisubtract(bucketNumber * _width, bottom);
            var max = min + (Int64)_width;
            if (max < min)
                max = Int64.MaxValue;
            return (min, max);
        }

        /// <summary>
        /// Returns IDs of all buckets that are after the bucket of <c>value</c>. Optionally excludes the bucket ID (if exists) for <c>value</c>.
        /// </summary>
        public List<Int64> GetBucketsGEQ(Int64 value, bool inclusive = true)
        {
            var res = new List<Int64>();

            var st_index = IndexGEQ(value);

            if (!inclusive)
                st_index += 1;

            for (var i = st_index; i < _bucketNos.Count; i++)
            {
                res.Add(_bucketIds[_bucketNos[i]]);
            }

            return res;
        }

        /// <summary>
        /// Returns IDs of all buckets that are before the bucket of <c>value</c>. Optionally excludes the bucket ID (if exists) for <c>value</c>.
        /// </summary>
        public List<Int64> GetBucketsLEQ(Int64 value, bool inclusive = true)
        {
            var res = new List<Int64>();

            var st_index = IndexLEQ(value);

            if (!inclusive)
                st_index -= 1;

            for (var i = 0; i <= st_index; i++)
            {
                res.Add(_bucketIds[_bucketNos[i]]);
            }

            return res;
        }

        /// <summary>
        /// Returns IDs of all buckets that are between the bucket of <c>value1</c> and the bucket of <c>value2</c>. Optionally excludes the bucket IDs (if exist) for <c>value1</c> and <c>value2</c>.
        /// </summary>
        public List<Int64> GetBucketsBetween(Int64 value1, Int64 value2, bool inclusive = true)
        {
            var res = new List<Int64>();

            if (value1 > value2) swap(ref value1, ref value2);

            var start_index = IndexGEQ(value1);
            var stop_index = IndexLEQ(value2);

            if (!inclusive)
            {
                start_index += 1;
                stop_index -= 1;
            }

            for (var i = start_index; i <= stop_index; i++)
            {
                res.Add(_bucketIds[_bucketNos[i]]);
            }

            return res;
        }

        private UInt64 _GetBucketNumber(Int64 value)
        {
            var bottom = uiabs(Int64.MinValue);
            var uivalue = uiabs(value);
            var diff = value > 0 ? bottom + uivalue : bottom - uivalue;

            return diff / _width;
        }

        private Int64 _GenerateBucketId(UInt64 bucketNo)
        {
            Int64 res;

            using (var xrng = RandomNumberGenerator.Create())
            {
                var counter = 0;
                const int tries = 10000;
                do
                {
                    if (counter++ > tries) throw new ApplicationException($"Can't generate new bucket after {tries} tries");
                    var b = new byte[8];
                    xrng.GetBytes(b);
                    res = BitConverter.ToInt64(b, 0);
                } while (_bucketIdList.ContainsKey(res));

                _bucketIdList[res] = 0;
                _bucketIds[bucketNo] = res;
                _bucketNos.Add(bucketNo);
            }

            return res;
        }

        /// <summary>
        /// Return index of the NEXT bucket. If there is no next bucket, returns <code>_bucketNos.Count</code>.
        /// </summary>
        private int IndexGEQ(Int64 value)
        {
            var bid = _GetBucketNumber(value);

            var (_, b) = _bucketNos.BinarySearch(bid);

            return b;
        }

        /// <summary>
        /// Return index of the PREV bucket. If there is no prev bucket, returns -1.
        /// </summary>
        private int IndexLEQ(Int64 value)
        {
            var bid = _GetBucketNumber(value);

            var (a, _) = _bucketNos.BinarySearch(bid);

            return a;
        }

        private static UInt64 uiabs(Int64 value)
        {
            return value > 0 ?
                (UInt64)value :
                (UInt64)(-(value + 1)) + 1;
        }

        private static Int64 uisubtract(UInt64 first, UInt64 second)
        {
            var diff = (second - first) - 1;
            return -((Int64)diff) - 1;
        }

        private static void swap(ref Int64 a, ref Int64 b)
        {
            var temp = a;
            a = b;
            b = temp;
        }
    }
}
