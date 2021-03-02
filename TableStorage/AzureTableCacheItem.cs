using System;
using Azure;
using Azure.Data.Tables;

namespace Azure.Extensions.Caching.Storage.Table
{
    public class AzureTableCacheItem : ITableEntity
    {
        /// <summary>
        /// Gets or sets the data.
        /// </summary>
        public string Data { get; set; }

        /// <summary>
        /// Gets or sets the options
        /// </summary>
        public string Options { get; set; }

        /// <summary>
        /// Gets or sets the absolute expiration date and time.
        /// </summary>
        public DateTimeOffset? AbsoluteExpiration { get; set; }

        /// <summary>
        /// Gets or sets the partition key
        /// </summary>
        public string PartitionKey { get; set; }

        /// <summary>
        /// Gets or sets the row key
        /// </summary>
        public string RowKey { get; set; }

        /// <summary>
        /// Gets or sets the created date and time.
        /// </summary>
        public DateTimeOffset? Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the etag
        /// </summary>
        public ETag ETag { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureCacheItem"/> class.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string of the storage account.
        /// </param>
        /// <param name="partitionKey">
        /// The partition key to use.
        /// </param>
        /// <param name="tableName">
        /// The name of the table to use. If the table doesn't exist it will be created.
        /// </param>
        public AzureTableCacheItem(string partititionKey, string rowKey, string data, string options) 
        {
            if (String.IsNullOrWhiteSpace(partititionKey))
                throw new ArgumentNullException("partitionKey");
                
            PartitionKey = partititionKey;

            if (String.IsNullOrWhiteSpace(rowKey))
                throw new ArgumentNullException("rowKey");

            RowKey = rowKey;

            if (data == null)
                throw new ArgumentNullException("data");

            Data = data;

            if (options == null)
                throw new ArgumentNullException("options");

            Options = options;
        }

        public AzureTableCacheItem() {}
    }
}