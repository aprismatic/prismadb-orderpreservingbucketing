using System;
using Xunit;
using PrismaDB.OrderPreservingBucketing;

namespace Tests
{
    public class ConcurrentSortedListTests
    {
        [Fact]
        public void AddAndContainsTest()
        {
            var csl = new ConcurrentSortedList();

            Assert.Equal(0, csl.Count);
            Assert.False(csl.Contains(12));

            csl.Add(12);
            Assert.Equal(1, csl.Count);
            Assert.True(csl.Contains(12));
            Assert.False(csl.Contains(15));

            csl.Add(15);
            Assert.Equal(2,csl.Count);
            Assert.True(csl.Contains(12));
            Assert.True(csl.Contains(15));
            Assert.False(csl.Contains(5));

            csl.Add(5);
            Assert.Equal(3,csl.Count);
            Assert.True(csl.Contains(12));
            Assert.True(csl.Contains(15));
            Assert.True(csl.Contains(5));
            Assert.False(csl.Contains(13));

            Assert.Equal((UInt64)5, csl[0]);
            Assert.Equal((UInt64)15,csl[2]);
            Assert.Equal((UInt64)12,csl[1]);

            csl.Add(12); // repeat
            Assert.Equal(3, csl.Count);
            Assert.True(csl.Contains(12));
            Assert.True(csl.Contains(15));
            Assert.True(csl.Contains(5));
            Assert.False(csl.Contains(13));

            Assert.Equal((UInt64)5, csl[0]);
            Assert.Equal((UInt64)12, csl[1]);
            Assert.Equal((UInt64)15, csl[2]);

            csl.Add(13);
            Assert.Equal(4, csl.Count);
            Assert.True(csl.Contains(12));
            Assert.True(csl.Contains(15));
            Assert.True(csl.Contains(5));
            Assert.True(csl.Contains(13));
            Assert.False(csl.Contains(100)); // random

            Assert.Equal((UInt64)5, csl[0]);
            Assert.Equal((UInt64)12, csl[1]);
            Assert.Equal((UInt64)13, csl[2]);
            Assert.Equal((UInt64)15, csl[3]);
        }

        [Fact]
        public void BinarySearchTest()
        {
            int a, b;
            var csl = new ConcurrentSortedList();

            (a, b) = csl.BinarySearch(1);

            Assert.Equal(-1, a);
            Assert.Equal(0, b);

            csl.Add(1);

            (a, b) = csl.BinarySearch(1);
            Assert.Equal(0, a);
            Assert.Equal(0, b);
            (a, b) = csl.BinarySearch(0);
            Assert.Equal(-1, a);
            Assert.Equal(0, b);
            (a, b) = csl.BinarySearch(100);
            Assert.Equal(csl.Count - 1, a);
            Assert.Equal(csl.Count, b);

            csl.Add(2);

            (a, b) = csl.BinarySearch(1);
            Assert.Equal(0, a);
            Assert.Equal(0, b);
            (a, b) = csl.BinarySearch(2);
            Assert.Equal(1, a);
            Assert.Equal(1, b);
            (a, b) = csl.BinarySearch(0);
            Assert.Equal(-1, a);
            Assert.Equal(0, b);
            (a, b) = csl.BinarySearch(100);
            Assert.Equal(csl.Count - 1, a);
            Assert.Equal(csl.Count, b);

            csl.Add(4);

            (a, b) = csl.BinarySearch(1);
            Assert.Equal(0, a);
            Assert.Equal(0, b);
            (a, b) = csl.BinarySearch(2);
            Assert.Equal(1, a);
            Assert.Equal(1, b);
            (a, b) = csl.BinarySearch(4);
            Assert.Equal(2, a);
            Assert.Equal(2, b);
            (a, b) = csl.BinarySearch(0);
            Assert.Equal(-1, a);
            Assert.Equal(0, b);
            (a, b) = csl.BinarySearch(100);
            Assert.Equal(csl.Count - 1, a);
            Assert.Equal(csl.Count, b);

            (a, b) = csl.BinarySearch(3);
            Assert.Equal(1, a);
            Assert.Equal(2, b);
        }
    }
}
