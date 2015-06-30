﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class SqlQueries
    {
        private const string CreateTableFormat = "CREATE TABLE {0}(" +
            // add collation to the key column to make it case-sensitive
            "Id nvarchar(900) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL, " +
            "Value varbinary(MAX) NOT NULL, " +
            "ExpiresAtTime datetimeoffset NOT NULL, " +
            "SlidingExpirationInTicks bigint NULL," +
            "AbsoluteExpiration datetimeoffset NULL, " +
            "CONSTRAINT pk_Id PRIMARY KEY (Id))";

        private const string CreateNonClusteredIndexOnExpirationTimeFormat
            = "CREATE NONCLUSTERED INDEX Index_ExpiresAtTime ON {0}(ExpiresAtTime)";

        private const string TableInfoFormat =
            "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_SCHEMA = '{0}' " +
            "AND TABLE_NAME = '{1}'";

        private const string GetCacheItemFormat =
            "SELECT Id, ExpiresAtTime, SlidingExpirationInTicks, AbsoluteExpiration, Value " +
            "FROM {0} WHERE Id = @Id AND @UtcNow <= ExpiresAtTime";

        private const string GetCacheItemExpirationInfoFormat =
            "SELECT Id, ExpiresAtTime, SlidingExpirationInTicks, AbsoluteExpiration " +
            "FROM {0} WHERE Id = @Id AND @UtcNow <= ExpiresAtTime";

        private const string SetCacheItemFormat =
            "IF NOT EXISTS(SELECT Id FROM {0} WHERE Id = @Id) " +
            "BEGIN " +
                "INSERT INTO {0} " +
                    "(Id, Value, ExpiresAtTime, SlidingExpirationInTicks, AbsoluteExpiration) " +
                    "VALUES (@Id, @Value, @ExpiresAtTime, @SlidingExpirationInTicks, @AbsoluteExpiration) " +
            "END " +
            "ELSE " +
            "BEGIN " +
                "UPDATE {0} SET Value = @Value, ExpiresAtTime = @ExpiresAtTime, " +
                "SlidingExpirationInTicks = @SlidingExpirationInTicks, AbsoluteExpiration = @AbsoluteExpiration " +
                "WHERE Id = @Id " +
            "END ";

        private const string DeleteCacheItemFormat = "DELETE FROM {0} WHERE Id = @Id";

        private const string UpdateCacheItemExpirationFormat = "UPDATE {0} SET ExpiresAtTime = @ExpiresAtTime " +
            "WHERE Id = @Id";

        public const string DeleteExpiredCacheItemsFormat = "DELETE FROM {0} WHERE @UtcNow > ExpiresAtTime";

        public SqlQueries(string schemaName, string tableName)
        {
            //TODO: sanitize schema and table name

            var tableNameWithSchema = string.Format("[{0}].[{1}]", schemaName, tableName);
            CreateTable = string.Format(CreateTableFormat, tableNameWithSchema);
            CreateNonClusteredIndexOnExpirationTime = string.Format(
                CreateNonClusteredIndexOnExpirationTimeFormat,
                tableNameWithSchema);
            TableInfo = string.Format(TableInfoFormat, schemaName, tableName);
            GetCacheItem = string.Format(GetCacheItemFormat, tableNameWithSchema);
            GetCacheItemExpirationInfo = string.Format(GetCacheItemExpirationInfoFormat, tableNameWithSchema);
            DeleteCacheItem = string.Format(DeleteCacheItemFormat, tableNameWithSchema);
            UpdateCacheItemExpiration = string.Format(UpdateCacheItemExpirationFormat, tableNameWithSchema);
            DeleteExpiredCacheItems = string.Format(DeleteExpiredCacheItemsFormat, tableNameWithSchema);
            SetCacheItem = string.Format(SetCacheItemFormat, tableNameWithSchema);
        }

        public string CreateTable { get; }

        public string CreateNonClusteredIndexOnExpirationTime { get; }

        public string GetTableSchema { get; }

        public string TableInfo { get; }

        public string GetCacheItem { get; }

        public string GetCacheItemExpirationInfo { get; }

        public string SetCacheItem { get; }

        public string DeleteCacheItem { get; }

        public string UpdateCacheItemExpiration { get; }

        public string DeleteExpiredCacheItems { get; }
    }
}
