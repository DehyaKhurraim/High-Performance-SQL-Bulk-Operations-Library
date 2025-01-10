using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInserion
{
    public static class PostgresBulk
    {
        public static async Task BulkInsertWithCopyAsync<T>(IEnumerable<T> entities, string tableName, NpgsqlConnection connection)
        {
            try
            {
                using var writer = connection.BeginBinaryImport($"COPY {tableName} FROM STDIN (FORMAT BINARY)");
                foreach (var entity in entities)
                {
                    writer.StartRow();
                    foreach (var property in typeof(T).GetProperties())
                    {
                        writer.Write(property.GetValue(entity));
                    }
                }
                await writer.CompleteAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during bulk insert: {ex.Message}");
            }
        }

        public static async Task BulkUpdateWithCopyAsync<T>(
            IEnumerable<T> entities,
            string tableName,
            string uniqueColumn,
            NpgsqlConnection connection)
        {
            try
            {
                // Update records in bulk based on the unique column (email)
                foreach (var entity in entities)
                {
                    var email = typeof(T).GetProperty(uniqueColumn)?.GetValue(entity)?.ToString();
                    if (email != null)
                    {
                        var updateCommand = $"UPDATE {tableName} SET ";
                        var propertiesToUpdate = typeof(T).GetProperties()
                            .Where(p => p.Name != uniqueColumn) // Skip the unique column
                            .Select(p => $"{p.Name} = @{p.Name}");

                        updateCommand += string.Join(", ", propertiesToUpdate) + $" WHERE {uniqueColumn} = @Email";

                        using var command = new NpgsqlCommand(updateCommand, connection);
                        command.Parameters.AddWithValue("Email", email);

                        foreach (var property in typeof(T).GetProperties())
                        {
                            if (property.Name != uniqueColumn)
                            {
                                command.Parameters.AddWithValue(property.Name, property.GetValue(entity) ?? DBNull.Value);
                            }
                        }

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during bulk update: {ex.Message}");
            }
        }

        public static async Task BulkDeleteWithCommandAsync<T>(
            IEnumerable<T> entities,
            string tableName,
            string uniqueColumn,
            NpgsqlConnection connection)
        {
            try
            {
                // Extract the values of the unique column from the entities
                var uniqueValues = entities
                    .Select(entity => typeof(T).GetProperty(uniqueColumn)?.GetValue(entity)?.ToString())
                    .Where(value => !string.IsNullOrEmpty(value))
                    .ToArray();

                // Check if there are values to delete
                if (!uniqueValues.Any())
                {
                    Console.WriteLine("No values to delete.");
                    return;
                }

                // Create a DELETE SQL command with uniqueColumn condition
                var deleteCommand = $"DELETE FROM {tableName} WHERE {uniqueColumn} = ANY(@UniqueValues)";

                using var command = new NpgsqlCommand(deleteCommand, connection);

                // Add the unique values as a parameter
                command.Parameters.AddWithValue("UniqueValues", uniqueValues);

                // Execute the DELETE command
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during bulk delete: {ex.Message}");
            }
        }

    }
}
