// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Internal;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class DatabaseOperations : IDatabaseOperations
    {
        public DatabaseOperations(
            string connectionString, string schemaName, string tableName, ISystemClock systemClock)
        {
            ConnectionString = connectionString;
            SchemaName = schemaName;
            TableName = tableName;
            SystemClock = systemClock;
            SqlQueries = new SqlQueries(schemaName, tableName);
        }

        protected SqlQueries SqlQueries { get; }

        protected string ConnectionString { get; }

        protected string SchemaName { get; }

        protected string TableName { get; }

        protected ISystemClock SystemClock { get; }

        public void DeleteCacheItem(string key)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddCacheItemId(key);

                connection.Open();

                command.ExecuteNonQuery();
            }
        }

        public async Task DeleteCacheItemAsync(string key)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddCacheItemId(key);

                await connection.OpenAsync();

                await command.ExecuteNonQueryAsync();
            }
        }

        public virtual byte[] GetCacheItem(string key)
        {
            return GetCacheItem(key, includeValue: true);
        }

        public virtual async Task<byte[]> GetCacheItemAsync(string key)
        {
            return await GetCacheItemAsync(key, includeValue: true);
        }

        public void RefreshCacheItem(string key)
        {
            GetCacheItem(key, includeValue: false);
        }

        public async Task RefreshCacheItemAsync(string key)
        {
            await GetCacheItemAsync(key, includeValue: false);
        }

        public virtual void GetTableSchema()
        {
            // Try connecting to the database and check if its available.
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.TableInfo, connection);
                connection.Open();

                var reader = command.ExecuteReader(CommandBehavior.SingleRow);
                if (!reader.Read())
                {
                    throw new InvalidOperationException(
                        $"Could not retrieve information of table with schema '{SchemaName}' and " +
                        $"name '{TableName}'. Make sure you have the table setup and try again. " +
                        $"Connection string: {ConnectionString}");
                }
            }
        }

        public virtual async Task GetTableSchemaAsync()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.TableInfo, connection);
                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                if (!await reader.ReadAsync())
                {
                    throw new InvalidOperationException(
                        $"Could not retrieve information of table with schema '{SchemaName}' and " +
                        $"name '{TableName}'. Make sure you have the table setup and try again. " +
                        $"Connection string: {ConnectionString}");
                }
            }
        }

        public void DeleteExpiredCacheItems()
        {
            var utcNowDateTime = SystemClock.UtcNow.UtcDateTime;

            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.DeleteExpiredCacheItems, connection);
                command.Parameters.AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                connection.Open();

                var effectedRowCount = command.ExecuteNonQuery();
            }
        }

        public virtual void SetCacheItem(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            var utcNowDateTime = SystemClock.UtcNow.UtcDateTime;

            var expirationInfo = CacheItemExpiration.GetExpirationInfo(utcNowDateTime, options);

            using (var connection = new SqlConnection(ConnectionString))
            {
                var upsertCommand = new SqlCommand(SqlQueries.SetCacheItem, connection);
                upsertCommand.Parameters
                    .AddCacheItemId(key)
                    .AddCacheItemValue(value)
                    .AddExpiresAtTime(expirationInfo.ExpiresAtTimeUTC)
                    .AddSlidingExpirationInTicks(expirationInfo.SlidingExpiration)
                    .AddAbsoluteExpiration(expirationInfo.AbsoluteExpirationUTC);

                connection.Open();

                try
                {
                    upsertCommand.ExecuteNonQuery();
                }
                catch (SqlException)
                {
                    // There is a possibility that multiple requests can try to add the same item to the cache, in
                    // which case we receive a 'duplicate key' exception on the primary key column.
                }
            }
        }

        public virtual async Task SetCacheItemAsync(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            var utcNowDateTime = SystemClock.UtcNow.UtcDateTime;

            var expirationInfo = CacheItemExpiration.GetExpirationInfo(utcNowDateTime, options);

            using (var connection = new SqlConnection(ConnectionString))
            {
                var upsertCommand = new SqlCommand(SqlQueries.SetCacheItem, connection);
                upsertCommand.Parameters
                    .AddCacheItemId(key)
                    .AddCacheItemValue(value)
                    .AddExpiresAtTime(expirationInfo.ExpiresAtTimeUTC)
                    .AddSlidingExpirationInTicks(expirationInfo.SlidingExpiration)
                    .AddAbsoluteExpiration(expirationInfo.AbsoluteExpirationUTC);

                await connection.OpenAsync();

                try
                {
                    await upsertCommand.ExecuteNonQueryAsync();
                }
                catch (SqlException)
                {
                    // There is a possibility that multiple requests can try to add the same item to the cache, in
                    // which case we receive a 'duplicate key' exception on the primary key column.
                }
            }
        }

        protected virtual byte[] GetCacheItem(string key, bool includeValue)
        {
            var utcNowDateTime = SystemClock.UtcNow.UtcDateTime;

            string query;
            if (includeValue)
            {
                query = SqlQueries.GetCacheItem;
            }
            else
            {
                query = SqlQueries.GetCacheItemExpirationInfo;
            }

            byte[] value = null;
            TimeSpan? slidingExpiration = null;
            DateTime? absoluteExpirationUTC = null;
            DateTime expirationTimeUTC;
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(query, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                connection.Open();

                var reader = command.ExecuteReader(
                    CommandBehavior.SequentialAccess | CommandBehavior.SingleRow | CommandBehavior.SingleResult);

                if (reader.Read())
                {
                    var id = reader.GetFieldValue<string>(Columns.Indexes.CacheItemIdIndex);

                    expirationTimeUTC = reader.GetFieldValue<DateTime>(Columns.Indexes.ExpiresAtTimeUTCIndex);

                    if (!reader.IsDBNull(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetFieldValue<long>(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    if (!reader.IsDBNull(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = reader.GetFieldValue<DateTime>(
                            Columns.Indexes.AbsoluteExpirationUTCIndex);
                    }

                    if (includeValue)
                    {
                        value = reader.GetFieldValue<byte[]>(Columns.Indexes.CacheItemValueIndex);
                    }
                }
                else
                {
                    return null;
                }
            }

            UpdateCacheItemExpiration(
                key, utcNowDateTime, expirationTimeUTC, slidingExpiration, absoluteExpirationUTC);

            return value;
        }

        protected virtual async Task<byte[]> GetCacheItemAsync(string key, bool includeValue)
        {
            var utcNowDateTime = SystemClock.UtcNow.UtcDateTime;

            string query;
            if (includeValue)
            {
                query = SqlQueries.GetCacheItem;
            }
            else
            {
                query = SqlQueries.GetCacheItemExpirationInfo;
            }

            byte[] value = null;
            TimeSpan? slidingExpiration = null;
            DateTime? absoluteExpirationUTC = null;
            DateTime expirationTimeUTC;
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(query, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess | CommandBehavior.SingleRow | CommandBehavior.SingleResult);

                if (await reader.ReadAsync())
                {
                    var id = await reader.GetFieldValueAsync<string>(Columns.Indexes.CacheItemIdIndex);

                    expirationTimeUTC = await reader.GetFieldValueAsync<DateTime>(
                        Columns.Indexes.ExpiresAtTimeUTCIndex);

                    if (!await reader.IsDBNullAsync(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            await reader.GetFieldValueAsync<long>(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    if (!await reader.IsDBNullAsync(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = await reader.GetFieldValueAsync<DateTime>(
                            Columns.Indexes.AbsoluteExpirationUTCIndex);
                    }

                    if (includeValue)
                    {
                        value = await reader.GetFieldValueAsync<byte[]>(Columns.Indexes.CacheItemValueIndex);
                    }
                }
                else
                {
                    return null;
                }
            }

            await UpdateCacheItemExpirationAsync(
                key, utcNowDateTime, expirationTimeUTC, slidingExpiration, absoluteExpirationUTC);

            return value;
        }

        protected virtual void UpdateCacheItemExpiration(
            string key,
            DateTime utcNowDateTime,
            DateTime currentExpirationTimeUTC,
            TimeSpan? slidingExpiration,
            DateTime? absoluteExpirationUTC)
        {
            var newExpirationTimeUTC = CacheItemExpiration.GetNewExpirationTime(
                    utcNowDateTime, currentExpirationTimeUTC, slidingExpiration, absoluteExpirationUTC);

            if (newExpirationTimeUTC.HasValue)
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    var command = new SqlCommand(SqlQueries.UpdateCacheItemExpiration, connection);
                    command.Parameters
                        .AddCacheItemId(key)
                        .AddExpiresAtTime(newExpirationTimeUTC.Value);

                    command.ExecuteNonQuery();
                }
            }
        }

        protected virtual async Task UpdateCacheItemExpirationAsync(
            string key,
            DateTime utcNowDateTime,
            DateTime currentExpirationTimeUTC,
            TimeSpan? slidingExpiration,
            DateTime? absoluteExpirationUTC)
        {
            var newExpirationTimeUTC = CacheItemExpiration.GetNewExpirationTime(
                    utcNowDateTime, currentExpirationTimeUTC, slidingExpiration, absoluteExpirationUTC);

            if (newExpirationTimeUTC.HasValue)
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    var command = new SqlCommand(SqlQueries.UpdateCacheItemExpiration, connection);
                    command.Parameters
                        .AddCacheItemId(key)
                        .AddExpiresAtTime(newExpirationTimeUTC.Value);

                    await connection.OpenAsync();

                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}