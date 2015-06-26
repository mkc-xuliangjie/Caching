// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Internal;
using Xunit;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class CacheItemExpirationTest
    {
        [Fact]
        public void AbsoluteExpirationRelativeToNow_SetsAbosluteExpirationAndExpiresAtTime()
        {
            // Arrange
            var timeSpan = TimeSpan.FromMinutes(20);
            var options = new DistributedCacheEntryOptions()
            {
                AbsoluteExpirationRelativeToNow = timeSpan,
                AbsoluteExpiration = null,
                SlidingExpiration = null
            };
            var testClock = new TestClock();

            // Act
            var expirationInfo = CacheItemExpiration.GetExpirationInfo(testClock.UtcDateTime, options);

            // Assert
            Assert.Equal(testClock.UtcDateTime + timeSpan, expirationInfo.AbsoluteExpirationUTC);
            Assert.Equal(expirationInfo.AbsoluteExpirationUTC, expirationInfo.ExpiresAtTimeUTC);
            Assert.False(expirationInfo.SlidingExpiration.HasValue);
        }

        [Fact]
        public void AbsoluteExpiration_SetsAbosluteExpirationAndExpiresAtTime()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpiration = testClock.UtcNow + TimeSpan.FromHours(1);
            var options = new DistributedCacheEntryOptions()
            {
                AbsoluteExpirationRelativeToNow = null,
                AbsoluteExpiration = absoluteExpiration,
                SlidingExpiration = null
            };

            // Act
            var expirationInfo = CacheItemExpiration.GetExpirationInfo(testClock.UtcDateTime, options);

            // Assert
            Assert.Equal(absoluteExpiration.UtcDateTime, expirationInfo.AbsoluteExpirationUTC);
            Assert.Equal(expirationInfo.AbsoluteExpirationUTC, expirationInfo.ExpiresAtTimeUTC);
            Assert.False(expirationInfo.SlidingExpiration.HasValue);
        }

        [Fact]
        public void SlidingExpiration_DoublesSlidingExpirationWindow()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromMinutes(20);
            var expectedValue = testClock.UtcDateTime
                + TimeSpan.FromTicks((CacheItemExpiration.ExpirationTimeMultiplier * slidingExpiration.Ticks));
            var options = new DistributedCacheEntryOptions()
            {
                AbsoluteExpirationRelativeToNow = null,
                AbsoluteExpiration = null,
                SlidingExpiration = slidingExpiration
            };

            // Act
            var expirationInfo = CacheItemExpiration.GetExpirationInfo(testClock.UtcDateTime, options);

            // Assert
            Assert.Equal(expectedValue, expirationInfo.ExpiresAtTimeUTC);
            Assert.Equal(slidingExpiration, expirationInfo.SlidingExpiration);
            Assert.False(expirationInfo.AbsoluteExpirationUTC.HasValue);
        }

        [Fact]
        public void BothSlidingExpirationAndAbsoluteExpirationSet_DoublesSlidingExpirationWindow()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromMinutes(20);
            var absoluteExpiration = testClock.UtcNow.AddHours(1);
            var expectedExpiresAtTimeUTC = testClock.UtcDateTime
                + TimeSpan.FromTicks((CacheItemExpiration.ExpirationTimeMultiplier * slidingExpiration.Ticks));
            var options = new DistributedCacheEntryOptions()
            {
                AbsoluteExpirationRelativeToNow = null,
                AbsoluteExpiration = absoluteExpiration,
                SlidingExpiration = slidingExpiration
            };

            // Act
            var expirationInfo = CacheItemExpiration.GetExpirationInfo(testClock.UtcDateTime, options);

            // Assert
            Assert.Equal(expectedExpiresAtTimeUTC, expirationInfo.ExpiresAtTimeUTC);
            Assert.Equal(slidingExpiration, expirationInfo.SlidingExpiration);
            Assert.Equal(absoluteExpiration.UtcDateTime, expirationInfo.AbsoluteExpirationUTC);
        }

        [Theory]
        [InlineData(20)]
        [InlineData(30)]
        [InlineData(50)]
        public void BothSlidingExpirationAndAbsoluteExpirationSet_DoestNotExceedAbsoluteExpiration(int minutes)
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromMinutes(20);
            var absoluteExpiration = testClock.UtcNow.AddHours(1);
            var expectedExpiresAtTimeUTC = absoluteExpiration.UtcDateTime;
            var options = new DistributedCacheEntryOptions()
            {
                AbsoluteExpirationRelativeToNow = null,
                AbsoluteExpiration = absoluteExpiration,
                SlidingExpiration = slidingExpiration
            };
            testClock.Add(TimeSpan.FromMinutes(minutes));

            // Act
            var expirationInfo = CacheItemExpiration.GetExpirationInfo(testClock.UtcDateTime, options);

            // Assert
            Assert.Equal(expectedExpiresAtTimeUTC, expirationInfo.ExpiresAtTimeUTC);
            Assert.Equal(slidingExpiration, expirationInfo.SlidingExpiration);
            Assert.Equal(absoluteExpiration.UtcDateTime, expirationInfo.AbsoluteExpirationUTC);
        }
    }
}
