// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;

namespace Microsoft.Framework.Caching.SqlServer
{
    // Since Mono currently does not have support for DateTimeOffset, we convert the times to datetime
    public static class MonoSqlParameterCollectionExtensions
    {
        public static SqlParameterCollection AddExpiresAtTimeMono(
            this SqlParameterCollection parameters,
            DateTimeOffset utcTime)
        {
            return parameters.AddWithValue(Columns.Names.ExpiresAtTime, SqlDbType.DateTime, utcTime.UtcDateTime);
        }


        public static SqlParameterCollection AddAbsoluteExpirationMono(
                    this SqlParameterCollection parameters,
                    DateTimeOffset? utcTime)
        {
            if (utcTime.HasValue)
            {
                return parameters.AddWithValue(
                    Columns.Names.AbsoluteExpiration, SqlDbType.DateTime, utcTime.Value.UtcDateTime);
            }
            else
            {
                return parameters.AddWithValue(
                Columns.Names.AbsoluteExpiration, SqlDbType.DateTime, DBNull.Value);
            }
        }
    }
}
