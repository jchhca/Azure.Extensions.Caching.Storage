using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Extensions.Caching.Distributed;

namespace Azure.Extensions.Caching.Storage.Table
{
    /// <summary>
    /// An <see cref="IDistributedCache"/> implementation to cache data in Azure table storage and Cosmos DB.
    /// </summary>
    /// <seealso cref="IDistributedCache"/>.
    public class AzureTableStorageCache : IDistributedCache
    {
        /// <summary>
        /// The storage account connection string.
        /// </summary>
        private readonly string _connectionString;

        /// <summary>
        /// The storage table name.
        /// </summary>
        private readonly string _tableName;

        /// <summary>
        /// The storage table partition key.
        /// </summary>
        private readonly string _partitionKey;

        /// <summary>
        /// The storage table service client.
        /// </summary>
        private readonly TableServiceClient _serviceClient;

        /// <summary>
        /// The storage table client.
        /// </summary>
        private readonly TableClient _tableClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureTableStorageCache"/> class.
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
        public AzureTableStorageCache(string connectionString, string partitionKey, string tableName)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException("connectionstring");

            if (String.IsNullOrWhiteSpace(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (String.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException("tableName");

            _tableName = tableName;
            _connectionString = connectionString;
            _partitionKey = partitionKey;
            
            _serviceClient = new TableServiceClient(_connectionString);

            _tableClient = _serviceClient.GetTableClient(_tableName);
            _tableClient.CreateIfNotExists();
        }

        /// <summary>
        /// Gets a value with the given key.
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        /// <returns>
        /// A <see cref="byte[]"/>.
        /// </returns>
        public byte[] Get(string key)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            return GetAsync(key).Result;
        }

        /// <summary>
        /// Gets a string value with the given key.
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        /// <returns>
        /// A <see cref="string"/>.
        /// </returns>
        public string GetString(string key)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            return GetStringAsync(key).Result;
        }

        /// <summary>
        /// Gets a value with the given key.
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        /// <param name="token">
        /// Optional: The <see cref="CancellationToken"/>.
        /// </param>
        /// <returns>
        /// A <see cref="byte[]"/>.
        /// </returns>
        public async Task<byte[]> GetAsync(string key, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            var data = await GetStringAsync(key, token);

            return Encoding.UTF8.GetBytes(data);
        }

        /// <summary>
        /// Gets a string value with the given key.
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        /// <param name="token">
        /// Optional: The <see cref="CancellationToken"/>.
        /// </param>
        /// <returns>
        /// A <see cref="string"/>.
        /// </returns>
        public async Task<string> GetStringAsync(string key, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            AzureTableCacheItem item = null;

            try
            {
                item = await _tableClient
                    .QueryAsync<AzureTableCacheItem>(ent => ent.PartitionKey == _partitionKey && ent.RowKey == key)
                    .SingleAsync();
            }
            catch
            {
                throw new Exception($"No Object Found For Key: {key}");
            }

            if (IsExpired(item))
            {
                await RemoveAsync(key);

                return null;
            }

            return item.Data;
        }

        /// <summary>
        /// Refreshes a value in the cache based on its key, resetting its sliding expiration
        /// timeout (if any).
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        public void Refresh(string key)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            RefreshAsync(key).Wait();
        }

        /// <summary>
        /// Refreshes a value in the cache based on its key, resetting its sliding expiration
        /// timeout (if any).
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        /// <param name="token">
        /// Optional: The <see cref="CancellationToken"/>.
        /// </param>
        /// <returns>
        /// A <see cref="Task"/>.
        /// </returns>
        public async Task RefreshAsync(string key, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            var item = await RetrieveAsync(key);

            if (item != null)
            {
                if (IsExpired(item))
                {
                    await RemoveAsync(key);

                    return;
                }

                var options = JsonSerializer.Deserialize<DistributedCacheEntryOptions>(item.Options);

                await SetStringAsync(key, item.Data, options, token);
            }
        }

        /// <summary>
        /// Removes a value with the given key.
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        public void Remove(string key)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            RemoveAsync(key).Wait();
        }

        /// <summary>
        /// Removes a value with the given key.
        /// </summary>
        /// <param name="key">
        /// A string identifying the requested value.
        /// </param>
        /// <param name="token">
        /// Optional: The <see cref="CancellationToken"/>.
        /// </param>
        public async Task RemoveAsync(string key, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            await _tableClient.DeleteEntityAsync(_partitionKey, key);
        }

        /// <summary>
        /// Sets a value with the given key.
        /// </summary>
        /// <param name="key">
        /// Sets a value with the given key.
        /// </param>
        /// <param name="value">
        /// The value to set in the cache.
        /// </param>
        /// <param name="options">
        /// The cache options for the value.
        /// </param>
        public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            if (value == null)
                throw new ArgumentNullException("options");

            SetAsync(key, value, options).Wait();
        }

        /// <summary>
        /// Sets a string value with the given key.
        /// </summary>
        /// <param name="key">
        /// Sets a value with the given key.
        /// </param>
        /// <param name="value">
        /// The value to set in the cache.
        /// </param>
        /// <param name="options">
        /// The cache options for the value.
        /// </param>
        public void SetString(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            if (value == null)
                throw new ArgumentNullException("options");

            var data = Encoding.UTF8.GetString(value);

            SetStringAsync(key, data, options).Wait();
        }

        /// <summary>
        /// Sets a value with the given key.
        /// </summary>
        /// <param name="key">
        /// Sets a value with the given key.
        /// </param>
        /// <param name="value">
        /// The value to set in the cache.
        /// </param>
        /// <param name="options">
        /// The cache options for the value.
        /// </param>
        /// <returns>
        /// A <see cref="Task"/>.
        /// </returns>
        public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            if (options == null)
                throw new ArgumentNullException("options");
            
            var data = Encoding.UTF8.GetString(value);

            await SetStringAsync(key, data, options, token);
        }

        /// <summary>
        /// Sets a value with the given key.
        /// </summary>
        /// <param name="key">
        /// Sets a value with the given key.
        /// </param>
        /// <param name="value">
        /// The value to set in the cache.
        /// </param>
        /// <param name="options">
        /// The cache options for the value.
        /// </param>
        /// <returns>
        /// A <see cref="Task"/>.
        /// </returns>
        public async Task SetStringAsync(string key, string value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            if (options == null)
                throw new ArgumentNullException("options");

            DateTimeOffset? absoluteExpiration = null;
            DateTimeOffset currentTime = DateTimeOffset.UtcNow;

            if (options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                absoluteExpiration = currentTime.Add(options.AbsoluteExpirationRelativeToNow.Value);
            }
            else if (options.AbsoluteExpiration.HasValue)
            {
                if (options.AbsoluteExpiration.Value <= currentTime)
                {
                    throw new ArgumentOutOfRangeException(
                       nameof(options.AbsoluteExpiration),
                       options.AbsoluteExpiration.Value,
                       "The absolute expiration value must be in the future.");
                }
                absoluteExpiration = options.AbsoluteExpiration;
            }

            var optionString = JsonSerializer.Serialize(options);

            var item = new AzureTableCacheItem(_partitionKey, key, value, optionString);

            if (absoluteExpiration.HasValue)
            {
                item.AbsoluteExpiration = absoluteExpiration;
            }

            await _tableClient.UpsertEntityAsync(item);
        }

        /// <summary>
        /// A <see cref="Task"/>.
        /// </summary>
        /// <param name="key"></param>
        /// <returns>
        /// A <see cref="Task{AzureCacheItem}"/>.
        /// </returns>
        private async Task<AzureTableCacheItem> RetrieveAsync(string key)
        {
            return await _tableClient
                .QueryAsync<AzureTableCacheItem>(ent => ent.PartitionKey == _partitionKey && ent.RowKey == key)
                .SingleAsync();
        }

        /// <summary>
        /// Checks whether the cached item should be deleted based on the expiration value.
        /// </summary>
        /// <param name="item">
        /// The <see cref="AzureCacheItem"/>.
        /// </param>
        /// <returns>
        /// <c>true</c> if the item should be deleted, <c>false</c> otherwise.
        /// </returns>
        private bool IsExpired(AzureTableCacheItem item)
        {
            DateTimeOffset currentTime = DateTimeOffset.UtcNow;

            if (item.AbsoluteExpiration != null &&
                item.AbsoluteExpiration.Value <= currentTime)
            {
                return true;
            }

            return false;
        }
    }
}
