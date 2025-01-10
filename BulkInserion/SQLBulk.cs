using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInserion
{
    public static class SQLBulk
    {
        public static async Task BulkInsertWithSqlBulkCopyAsync<T>(IEnumerable<T> entities, string tableName, SqlConnection connection)
        {
            var dataTable = ConvertToDataTable(entities);

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName,
                BatchSize = 1000 // Set batch size for optimal performance
            };

            try
            {
                await bulkCopy.WriteToServerAsync(dataTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during bulk insert in SQL Server: {ex.Message}");
            }
        }

        private static DataTable ConvertToDataTable<T>(IEnumerable<T> entities)
        {
            var dataTable = new DataTable();
            var properties = typeof(T).GetProperties();

            foreach (var property in properties)
            {
                dataTable.Columns.Add(property.Name, Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }

            foreach (var entity in entities)
            {
                var row = dataTable.NewRow();
                foreach (var property in properties)
                {
                    row[property.Name] = property.GetValue(entity) ?? DBNull.Value;
                }
                dataTable.Rows.Add(row);
            }

            return dataTable;
        }




    }
}
