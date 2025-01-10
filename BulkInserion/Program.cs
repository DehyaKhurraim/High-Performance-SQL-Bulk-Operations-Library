using BulkInserion;
using CsvHelper;
using Npgsql;
using Npgsql.Internal;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Formats.Asn1;
using System.Globalization;
using System.Xml;


async Task ProcessCsvInParallelAsync<T>(
    string filePath,
    string tableName,
    int batchSize,
    int maxDegreeOfParallelism,
    string databaseType,
    string operationType,
    string keyColumn)
    where T : class, new()
{
    // Create a single connection
    var _connectionString = "Host=10.0.12.60;Port=5432;Database=BulkOperation;Username=postgres;Password=123";
    using var connection = new NpgsqlConnection(_connectionString);
    await connection.OpenAsync(); // Open a single connection

    // Use a thread-safe collection for parallel processing
    var batchQueue = new BlockingCollection<List<T>>();

    // Task to read the CSV in chunks and add to the queue
    var readingTask = Task.Run(() =>
    {
        foreach (var batch in ReadCsvInBatches<T>(filePath, batchSize))
        {
            batchQueue.Add(batch);
        }
        batchQueue.CompleteAdding();
    });

    // Task to process the batches in parallel
    var processingTask = Task.Run(async () =>
    {
        var stopwatch = new Stopwatch();
        await Parallel.ForEachAsync(batchQueue.GetConsumingEnumerable(), new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism
        }, async (batch, _) =>
        {

            switch (databaseType.ToLower())
            {
                case "postgres":
                    var npgsqlConnection = DatabaseConnectionFactory.GetPostgresConnection(_connectionString);
                    await npgsqlConnection.OpenAsync();
                    stopwatch = Stopwatch.StartNew();
                    switch (operationType.ToLower())
                    {
                        case "insert":
                            await PostgresBulk.BulkInsertWithCopyAsync(batch, tableName, npgsqlConnection); // Pass the shared connection
                            break;

                        case "update":
                            await PostgresBulk.BulkUpdateWithCopyAsync(batch, tableName, keyColumn, npgsqlConnection);
                            break;

                        case "delete":
                            await PostgresBulk.BulkDeleteWithCommandAsync(batch, tableName, keyColumn, npgsqlConnection);
                            break;

                        default:
                            throw new ArgumentException($"Operation type {operationType} is not supported for PostgreSQL.");
                    }
                    break;

                case "sqlserver":
                    var sqlConnection = DatabaseConnectionFactory.GetSqlServerConnection(_connectionString);
                    await sqlConnection.OpenAsync();
                    stopwatch = Stopwatch.StartNew();
                    switch (operationType.ToLower())
                    {
                        case "insert":
                            await SQLBulk.BulkInsertWithSqlBulkCopyAsync(batch, tableName, sqlConnection);
                            break;

                        case "update":
                            //await SQLBulk.BulkUpdateSqlServerAsync(batch, tableName, sqlConnection, keyColumn);
                            break;

                        case "delete":
                            //await SQLBulk.BulkDeleteSqlServerAsync(batch, tableName, sqlConnection, keyColumn);
                            break;

                        default:
                            throw new ArgumentException($"Operation type {operationType} is not supported for SQL Server.");
                    }
                    break;

                case "mysql":
                    //var mySqlConnection = DatabaseConnectionFactory.GetMySqlConnection(_connectionString);
                    //await mySqlConnection.OpenAsync();
                    stopwatch = Stopwatch.StartNew();
                    switch (operationType.ToLower())
                    {
                        case "insert":
                            //await MySQLBulk.BulkInsertMySqlAsync(batch, tableName, mySqlConnection);
                            break;

                        case "update":
                            //await MySQLBulk.BulkUpdateMySqlAsync(batch, tableName, mySqlConnection, keyColumn);
                            break;

                        case "delete":
                            //await MySQLBulk.BulkDeleteMySqlAsync(batch, tableName, mySqlConnection, keyColumn);
                            break;

                        default:
                            throw new ArgumentException($"Operation type {operationType} is not supported for MySQL.");
                    }
                    break;

                case "oracle":
                    var oracleConnection = DatabaseConnectionFactory.GetOracleServerConnection(_connectionString);
                    await oracleConnection.OpenAsync();
                    stopwatch = Stopwatch.StartNew();
                    switch (operationType.ToLower())
                    {
                        case "insert":
                            await OracleBulk.BulkInsertOracleAsync(batch, tableName, oracleConnection);
                            break;

                        case "update":
                            //await OracleBulk.BulkUpdateOracleAsync(batch, tableName, oracleConnection, keyColumn);
                            break;

                        case "delete":
                            //await OracleBulk.BulkDeleteOracleAsync(batch, tableName, oracleConnection, keyColumn);
                            break;

                        default:
                            throw new ArgumentException($"Operation type {operationType} is not supported for Oracle.");
                    }
                    break;

                case "sqlite":
                    //var sqliteConnection = DatabaseConnectionFactory.GetSQLiteConnection(_connectionString);
                    //await sqliteConnection.OpenAsync();
                    stopwatch = Stopwatch.StartNew();
                    switch (operationType.ToLower())
                    {
                        case "insert":
                            //await SQLiteBulk.BulkInsertSQLiteAsync(batch, tableName, sqliteConnection);
                            break;

                        case "update":
                            //await SQLiteBulk.BulkUpdateSQLiteAsync(batch, tableName, sqliteConnection, keyColumn);
                            break;

                        case "delete":
                            //await SQLiteBulk.BulkDeleteSQLiteAsync(batch, tableName, sqliteConnection, keyColumn);
                            break;

                        default:
                            throw new ArgumentException($"Operation type {operationType} is not supported for SQLite.");
                    }
                    break;

                default:
                    throw new NotSupportedException($"Database type {databaseType} is not supported.");
            }

        });
        Console.WriteLine("Time to execute:");
        stopwatch.Stop();
        Console.WriteLine(stopwatch);
    });

    // Wait for both reading and processing to complete
    await Task.WhenAll(readingTask, processingTask);
}

IEnumerable<List<T>> ReadCsvInBatches<T>(string filePath, int batchSize) where T : class, new()
{
    using var reader = new StreamReader(filePath);
    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

    var buffer = new List<T>();

    while (csv.Read())
    {
        var record = csv.GetRecord<T>();
        buffer.Add(record);

        if (buffer.Count == batchSize)
        {
            yield return buffer;
            buffer = new List<T>();
        }
    }

    if (buffer.Any())
    {
        yield return buffer;
    }
}

//var stopwatch = Stopwatch.StartNew();
await ProcessCsvInParallelAsync<User>(
    filePath: "large_dummy_data.csv",
    tableName: "users",
    batchSize: 10000,
    maxDegreeOfParallelism: 1,
    databaseType: "postgres",
    operationType: "insert",
    keyColumn: "email"
);
//stopwatch.Stop();
//Console.WriteLine(stopwatch);
