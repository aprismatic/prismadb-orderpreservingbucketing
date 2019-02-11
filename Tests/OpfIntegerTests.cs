using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using PrismaDB.OrderPreservingBucketing;

namespace Tests
{
    public class OpfIntegerTests
    {
        [Fact]
        public void SimpleTest()
        {
            var oi = new OpfInteger(100);

            var first = oi.GetBucketId(-123);
            var second = oi.GetBucketId(321);
            var third = oi.GetBucketId(890);

            var gt50 = oi.GetBucketsGT(50);

            Assert.Equal(2, gt50.Count);
            Assert.Contains(second, gt50);
            Assert.Contains(third, gt50);

            var lt50 = oi.GetBucketsLT(50);
            Assert.Single(lt50);
            Assert.Contains(first, lt50);

            var bt50_500 = oi.GetBucketsBetween(50, 500);
            Assert.Single(bt50_500);
            Assert.Contains(second, bt50_500);
        }

        [Fact]
        public void RepeatedReadCorrectness()
        {
            const int iters = 50000;
            var dict = new Dictionary<long, long>();
            var rnd = new Random();
            var oi = new OpfInteger(100);

            for (var i = 0; i < iters; i++)
            {
                var next = (long)((rnd.NextDouble() * 2.0 - 1.0) * long.MaxValue);
                dict[next] = oi.GetBucketId(next);
            }

            foreach (var key in dict.Keys)
            {
                Assert.Equal(dict[key], oi.GetBucketId(key));
            }
        }

        [Fact]
        public void GTCorrectness()
        {
            const int iters = 2000;
            var dict = new SortedDictionary<long, long>();
            var rnd = new Random();
            var oi = new OpfInteger(100);

            for (var i = 0; i < iters; i++)
            {
                var next = (long)((rnd.NextDouble() * 2.0 - 1.0) * long.MaxValue);
                dict[next] = oi.GetBucketId(next);
            }

            var keys = dict.Keys.ToList();
            for (var i = 0; i < keys.Count; i++)
            {
                var item = keys[i];
                var res = oi.GetBucketsGT(item);

                Assert.Equal(keys.Skip(i)
                                 .Where(x => oi.GetBucketRange(item).MaxValue != x)
                                 .Select(x => dict[x])
                                 .OrderBy(x => x)
                                 .Distinct(),
                             res.OrderBy(x => x));
            }
        }

        [Fact]
        public void LTCorrectness()
        {
            const int iters = 2000;
            var dict = new SortedDictionary<long, long>();
            var rnd = new Random();
            var oi = new OpfInteger(100);

            for (var i = 0; i < iters; i++)
            {
                var next = (long)((rnd.NextDouble() * 2.0 - 1.0) * long.MaxValue);
                dict[next] = oi.GetBucketId(next);
            }

            var keys = dict.Keys.ToList();
            for (var i = 0; i < keys.Count; i++)
            {
                var item = keys[i];
                var res = oi.GetBucketsLT(item);

                Assert.Equal(keys.Take(i + 1)
                                 .Where(x => oi.GetBucketRange(item).MinValue != x)
                                 .Select(x => dict[x])
                                 .OrderBy(x => x)
                                 .Distinct(),
                             res.OrderBy(x => x));
            }
        }

        [Fact]
        public void BetweenCorrectness()
        {
            const int iters = 200;
            var dict = new SortedDictionary<long, long>();
            var rnd = new Random();
            var oi = new OpfInteger(100);

            for (var i = 0; i < iters; i++)
            {
                var next = (long)((rnd.NextDouble() * 2.0 - 1.0) * long.MaxValue);
                dict[next] = oi.GetBucketId(next);
            }

            var keys = dict.Keys.ToList();
            for (var i = 0; i < keys.Count; i++)
            {
                for (var j = keys.Count - 1; j >= 0; j--)
                {
                    var item1 = keys[i];
                    var item2 = keys[j];
                    var res = oi.GetBucketsBetween(item1, item2);
                    var expected = oi.GetBucketsGT(Math.Min(item1, item2))
                        .Intersect(oi.GetBucketsLT(Math.Max(item1, item2)));

                    Assert.Equal(expected.OrderBy(x => x), res.OrderBy(x => x));
                }
            }
        }

        [Fact]
        public void BucketRangeCorrectness()
        {
            var oi = new OpfInteger(100);

            for (var i = Int64.MinValue; i <= Int64.MinValue + 5000; i++)
            {
                var range = oi.GetBucketRange(i);
                Assert.True(range.MinValue <= i);
                Assert.True(range.MaxValue >= i);
                if (range.MinValue == Int64.MinValue)
                {
                    Assert.True(range.IsFirstBucket);
                    Assert.False(range.IsLastBucket);
                }
                else
                {
                    Assert.False(range.IsFirstBucket);
                    Assert.False(range.IsLastBucket);
                }
            }

            for (var i = -2500; i <= 2500; i++)
            {
                var range = oi.GetBucketRange(i);
                Assert.True(range.MinValue <= i);
                Assert.True(range.MaxValue >= i);
                Assert.False(range.IsFirstBucket);
                Assert.False(range.IsLastBucket);
            }

            for (var i = Int64.MaxValue; i >= Int64.MaxValue - 5000; i--)
            {
                var range = oi.GetBucketRange(i);
                Assert.True(range.MinValue <= i);
                Assert.True(range.MaxValue >= i);
                if (range.MaxValue == Int64.MaxValue)
                {
                    Assert.False(range.IsFirstBucket);
                    Assert.True(range.IsLastBucket);
                }
                else
                {
                    Assert.False(range.IsFirstBucket);
                    Assert.False(range.IsLastBucket);
                }
            }
        }
    }
}
