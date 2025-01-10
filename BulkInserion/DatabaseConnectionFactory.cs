using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInserion
{
    public static class DatabaseConnectionFactory
    {
        // Get PostgreSQL connection
        public static NpgsqlConnection GetPostgresConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }

        // Get SQL Server connection
        public static SqlConnection GetSqlServerConnection(string connectionString)
        {
            return new SqlConnection(connectionString);
        }
        public static OracleConnection GetOracleServerConnection(string connectionString)
        {
            return new OracleConnection(connectionString);
        }
    }
}
