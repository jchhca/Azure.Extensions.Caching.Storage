using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Caching.Distributed;

namespace Azure.Extensions.Caching.Storage.Blob
{
    /// <summary>
    /// An <see cref="IDistributedCache"/> implementation to cache data in Azure blob storage
    /// </summary>
    /// <seealso cref="IDistributedCache"/>.
    public class AzureBlobStorageCache : IDistributedCache
    {
        /// <summary>
        /// The storage account connection string.
        /// </summary>
        private readonly string _connectionString;

        /// <summary>
        /// The storage container name.
        /// </summary>
        private readonly string _containerName;

        /// <summary>
        /// The storage blob container client
        /// </summary>
        private readonly BlobContainerClient _containerClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureTableStorageCache"/> class.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string of the storage account.
        /// </param>
        /// <param name="containerName">
        /// The container name to use.
        /// </param>
        public AzureBlobStorageCache(string connectionString, string containerName)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException("connectionstring");

            if (String.IsNullOrWhiteSpace(containerName))
                throw new ArgumentNullException("containerName");

            _connectionString = connectionString;
            _containerName = containerName;

            _containerClient = new BlobContainerClient(connectionString, containerName);
            _containerClient.CreateIfNotExists();
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

            var blob = _containerClient.GetBlobClient(key);

            var download = await blob.DownloadAsync();

            if (download == null)
            {
                throw new Exception($"No Object Found For Key: {key}");
            }

            using (var reader = new StreamReader(download.Value.Content))
            {
                return reader.ReadToEnd();
            }
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

            var value = await GetStringAsync(key);

            if (value != null)
            {
                await SetStringAsync(key, value, token);
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

            await _containerClient.DeleteBlobAsync(key);
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
        public void Set(string key, byte[] value, DistributedCacheEntryOptions options = null)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            Set(key, value);
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
        public void Set(string key, byte[] value)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            SetAsync(key, value).Wait();
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
        public void SetString(string key, byte[] value)
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            var data = Encoding.UTF8.GetString(value);

            SetStringAsync(key, data).Wait();
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

            await SetAsync(key, value, token);
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
        /// <returns>
        /// A <see cref="Task"/>.
        /// </returns>
        public async Task SetAsync(string key, byte[] value, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");
            
            var data = Encoding.UTF8.GetString(value);

            await SetStringAsync(key, data, token);
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
        /// <returns>
        /// A <see cref="Task"/>.
        /// </returns>
        public async Task SetStringAsync(string key, string value, CancellationToken token = default(CancellationToken))
        {
            if (String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            var blob = _containerClient.GetBlobClient(key);

            var byteArray = Encoding.UTF8.GetBytes(value);

            using (var memoryStream = new MemoryStream(byteArray))
            {
                await blob.UploadAsync(memoryStream);
            }
        }
    }
}
