namespace Azure.Extensions.Caching.Storage.Blob
{
    public class AzureBlobStorageCacheOptions
    {
        /// <summary>
        /// Gets or sets the connection string of the storage account.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the container name to use.
        /// </summary>
        public string ContainerName { get; set; }
    }
}
