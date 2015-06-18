// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Internal;
using Microsoft.Framework.OptionsModel;

namespace Microsoft.Framework.Caching.SqlServer
{
    /// <summary>
    /// Distributed cache implementation using Microsoft SQL Server database.
    /// </summary>
    public class SqlServerCache : IDistributedCache
    {
        private readonly SqlServerCacheOptions _options;
        private readonly IDatabaseOperations _dbOperations;
        private DateTime _lastExpirationScanUTC;

        public SqlServerCache(IOptions<SqlServerCacheOptions> options)
        {
            _options = options.Options;

            if(string.IsNullOrEmpty(_options.ConnectionString))
            {
                throw new ArgumentException(
                    $"{nameof(SqlServerCacheOptions.ConnectionString)} cannot be empty or null.");
            }
            if (string.IsNullOrEmpty(_options.SchemaName))
            {
                throw new ArgumentException(
                    $"{nameof(SqlServerCacheOptions.SchemaName)} cannot be empty or null.");
            }
            if (string.IsNullOrEmpty(_options.TableName))
            {
                throw new ArgumentException(
                    $"{nameof(SqlServerCacheOptions.TableName)} cannot be empty or null.");
            }

            // SqlClient library on Mono doesn't have support for DateTimeOffset and also
            // it doesn't have support for apis like GetFieldValue, GetFieldValueAsync etc.
            // So we detect the platform to perform things differently for Mono vs. non-Mono platforms.
            if (PlatformHelper.IsMono)
            {
                _dbOperations = new MonoDatabaseOperations(options.Options);
            }
            else
            {
                _dbOperations = new DatabaseOperations(options.Options);
            }
        }

        public void Connect()
        {
            // Try connecting to the database and check if its available.
            _dbOperations.GetTableSchema();
        }

        public async Task ConnectAsync()
        {
            // Try connecting to the database and check if its available.
            await _dbOperations.GetTableSchemaAsync();
        }

        public byte[] Get([NotNull] string key)
        {
            var value = _dbOperations.GetCacheItem(key);

            ScanForExpiredItemsIfRequired();

            return value;
        }

        public async Task<byte[]> GetAsync([NotNull] string key)
        {
            var value = await _dbOperations.GetCacheItemAsync(key);

            ScanForExpiredItemsIfRequired();

            return value;
        }

        public void Refresh([NotNull] string key)
        {
            _dbOperations.RefreshCacheItem(key);

            ScanForExpiredItemsIfRequired();
        }

        public async Task RefreshAsync([NotNull] string key)
        {
            await _dbOperations.RefreshCacheItemAsync(key);

            ScanForExpiredItemsIfRequired();
        }

        public void Remove([NotNull] string key)
        {
            _dbOperations.DeleteCacheItem(key);

            ScanForExpiredItemsIfRequired();
        }

        public async Task RemoveAsync([NotNull] string key)
        {
            await _dbOperations.DeleteCacheItemAsync(key);

            ScanForExpiredItemsIfRequired();
        }

        public void Set([NotNull] string key, [NotNull] byte[] value, [NotNull] DistributedCacheEntryOptions options)
        {
            _dbOperations.SetCacheItem(key, value, options);

            ScanForExpiredItemsIfRequired();
        }

        public async Task SetAsync(
            [NotNull] string key,
            [NotNull] byte[] value,
            [NotNull] DistributedCacheEntryOptions options)
        {
            await _dbOperations.SetCacheItemAsync(key, value, options);

            ScanForExpiredItemsIfRequired();
        }

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        private void ScanForExpiredItemsIfRequired()
        {
            var utcNow = _options.SystemClock.UtcNow.UtcDateTime;
            if ((utcNow - _lastExpirationScanUTC) > _options.ExpiredItemsDeletionInterval)
            {
                _lastExpirationScanUTC = utcNow;
                ThreadPool.QueueUserWorkItem(DeleteExpiredCacheItems, state: null);
            }
        }

        private void DeleteExpiredCacheItems(object state)
        {
            _dbOperations.DeleteExpiredCacheItems();
        }
    }
}