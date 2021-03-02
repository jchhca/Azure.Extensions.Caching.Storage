namespace Azure.Extensions.Caching.Storage.Table
{
    public class AzureTableStorageCacheOptions
    {
        /// <summary>
        /// Gets or sets the connection string of the storage account.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the name of the table to use.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the partition key to use.
        /// </summary>
        public string PartitionKey { get; set; }
    }
}
