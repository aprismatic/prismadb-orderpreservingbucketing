using System;
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

            var gt50 = oi.GetBucketsGEQ(50);

            Assert.Equal(2, gt50.Count);
            Assert.Contains(second, gt50);
            Assert.Contains(third, gt50);

            var lt50 = oi.GetBucketsLEQ(50);
            Assert.Single(lt50);
            Assert.Contains(first, lt50);

            var bt50_500 = oi.GetBucketsBetween(50, 500);
            Assert.Single(bt50_500);
            Assert.Contains(second, bt50_500);
        }
    }
}
