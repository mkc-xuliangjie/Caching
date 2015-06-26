// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Framework.Internal;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class MonoDatabaseOperations : DatabaseOperations
    {
        public MonoDatabaseOperations(
            string connectionString, string schemaName, string tableName, ISystemClock systemClock)
            : base(connectionString, schemaName, tableName, systemClock)
        {
        }

        protected override byte[] GetCacheItem(string key, bool includeValue)
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

                var reader = command.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SingleResult);

                if (reader.Read())
                {
                    var id = reader.GetString(Columns.Indexes.CacheItemIdIndex);

                    expirationTimeUTC = DateTime.Parse(reader[Columns.Indexes.ExpiresAtTimeUTCIndex].ToString());

                    if (!reader.IsDBNull(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetInt64(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    if (!reader.IsDBNull(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = DateTime.Parse(
                            reader[Columns.Indexes.AbsoluteExpirationUTCIndex].ToString());
                    }

                    if (includeValue)
                    {
                        value = (byte[])reader[Columns.Indexes.CacheItemValueIndex];
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

        protected override async Task<byte[]> GetCacheItemAsync(string key, bool includeValue)
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
                var command = new SqlCommand(SqlQueries.GetCacheItem, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(
                    CommandBehavior.SingleRow | CommandBehavior.SingleResult);

                if (await reader.ReadAsync())
                {
                    var id = reader.GetString(Columns.Indexes.CacheItemIdIndex);

                    expirationTimeUTC = DateTime.Parse(reader[Columns.Indexes.ExpiresAtTimeUTCIndex].ToString());

                    if (!await reader.IsDBNullAsync(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            Convert.ToInt64(reader[Columns.Indexes.SlidingExpirationInTicksIndex].ToString()));
                    }

                    if (!await reader.IsDBNullAsync(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = DateTime.Parse(
                            reader[Columns.Indexes.AbsoluteExpirationUTCIndex].ToString());
                    }

                    if (includeValue)
                    {
                        value = (byte[])reader[Columns.Indexes.CacheItemValueIndex];
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
    }
}