using BulkInserion;
using Npgsql;

namespace UnitTest
{
    [TestClass]
    public class UnitTest1
    {
        private const string PostgresConnectionString = "Host=10.0.12.60;Port=5432;Database=BulkOperation;Username=postgres;Password=123";
        private const string TestTableName = "users";

        public class TestUser
        {
            public string first_name { get; set; }
            public string last_name { get; set; }
            public string email { get; set; }
            public string address { get; set; }
            public string gender { get; set; }
            public int age { get; set; }
            public string occupation { get; set; }
        }

        [TestMethod]
        public async Task BulkInsert_Postgres_ShouldInsertRecordsSuccessfully()
        {
            // Arrange
            var users = GenerateDummyData(10000);
            using var connection = new NpgsqlConnection(PostgresConnectionString);
            await connection.OpenAsync();

            // Act
            await PostgresBulk.BulkInsertWithCopyAsync(users, TestTableName, connection);

            // Assert
            using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {TestTableName}", connection);
            var count = (long)await command.ExecuteScalarAsync();
            Assert.AreEqual(users.Count, count);
        }

        //[Fact]
        //public async Task BulkUpdate_Postgres_ShouldUpdateRecordsSuccessfully()
        //{
        //    // Arrange
        //    var users = GenerateDummyData(10000);
        //    var updatedUsers = users.Select(u => { u.first_name = "UpdatedName"; return u; }).ToList();
        //    using var connection = new NpgsqlConnection(PostgresConnectionString);
        //    await connection.OpenAsync();
        //    await PostgresBulk.BulkInsertWithCopyAsync(users, TestTableName, connection);

        //    // Act
        //    await PostgresBulk.BulkUpdatePostgresAsync(updatedUsers, TestTableName, connection, "email");

        //    // Assert
        //    using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {TestTableName} WHERE first_name = 'UpdatedName'", connection);
        //    var count = (long)await command.ExecuteScalarAsync();
        //    Assert.Equal(users.Count, count);
        //}

        //[Fact]
        //public async Task BulkDelete_Postgres_ShouldDeleteRecordsSuccessfully()
        //{
        //    // Arrange
        //    var users = GenerateDummyData(10000);
        //    using var connection = new NpgsqlConnection(PostgresConnectionString);
        //    await connection.OpenAsync();
        //    await PostgresBulk.BulkInsertWithCopyAsync(users, TestTableName, connection);

        //    // Act
        //    await PostgresBulk.BulkDeletePostgresAsync(users, TestTableName, connection, "email");

        //    // Assert
        //    using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {TestTableName}", connection);
        //    var count = (long)await command.ExecuteScalarAsync();
        //    Assert.Equal(0, count);
        //}

        //[Fact]
        //public async Task ProcessCsvInParallel_ShouldProcessRecordsEfficiently()
        //{
        //    // Arrange
        //    var filePath = "test_large_data.csv";
        //    GenerateCsvFile(filePath, 100000); // Generate a test CSV file with 100k records

        //    // Act
        //    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        //    await ProcessCsvInParallelAsync<TestUser>(
        //        filePath,
        //        TestTableName,
        //        batchSize: 10000,
        //        maxDegreeOfParallelism: 4,
        //        databaseType: "postgres",
        //        operationType: "insert",
        //        keyColumn: "email"
        //    );
        //    stopwatch.Stop();

        //    // Assert
        //    Assert.True(stopwatch.ElapsedMilliseconds < 60000); // Ensure the operation completes in less than 60 seconds
        //}

        //[Fact]
        //public async Task InvalidDatabaseType_ShouldThrowNotSupportedException()
        //{
        //    // Arrange
        //    var filePath = "test_invalid_data.csv";
        //    GenerateCsvFile(filePath, 100);

        //    // Act & Assert
        //    await Assert.ThrowsAsync<NotSupportedException>(async () =>
        //    {
        //        await ProcessCsvInParallelAsync<TestUser>(
        //            filePath,
        //            TestTableName,
        //            batchSize: 100,
        //            maxDegreeOfParallelism: 1,
        //            databaseType: "invalid_db",
        //            operationType: "insert",
        //            keyColumn: "email"
        //        );
        //    });
        //}

        // Helper Methods
        private List<TestUser> GenerateDummyData(int count)
        {
            return Enumerable.Range(1, count).Select(i => new TestUser
            {
                first_name = $"First{i}",
                last_name = $"Last{i}",
                email = $"user{i}@example.com",
                address = $"Address{i}",
                gender = i % 2 == 0 ? "Male" : "Female",
                age = i % 100,
                occupation = $"Occupation{i}"
            }).ToList();
        }

        private void GenerateCsvFile(string filePath, int count)
        {
            using var writer = new StreamWriter(filePath);
            writer.WriteLine("first_name,last_name,email,address,gender,age,occupation");

            foreach (var user in GenerateDummyData(count))
            {
                writer.WriteLine($"{user.first_name},{user.last_name},{user.email},{user.address},{user.gender},{user.age},{user.occupation}");
            }
        }
    }
}