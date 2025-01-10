using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInserion
{
    public static class OracleBulk
    {
        public static async Task BulkInsertOracleAsync<T>(IEnumerable<T> entities, string tableName, OracleConnection connection)
        {
            try
            {
                // Convert entities to a DataTable
                var dataTable = ConvertToDataTable(entities);

                using var bulkCopy = new OracleBulkCopy(connection)
                {
                    DestinationTableName = tableName,
                    BulkCopyTimeout = 600
                };

                foreach (var column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());
                }

                bulkCopy.WriteToServer(dataTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during bulk insert in Oracle: {ex.Message}");
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
