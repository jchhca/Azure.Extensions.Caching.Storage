using System;
using Azure.Extensions.Caching.Storage.Blob;
using Azure.Extensions.Caching.Storage.Table;
using Microsoft.Extensions.Caching.Distributed;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    /// Extension methods to add Azure table storage cache.
    /// </summary>
    public static class AzureStorageCacheExtensions
    {
        /// <summary>
        /// Add Azure blob storage cache as an IDistributedCache to the service container.
        /// </summary>
        /// <param name="services">
        /// The <see cref="IServiceCollection"/>.
        /// </param>
        /// <param name="options">
        /// The <see cref="AzureBlobStorageCacheOptions"/>.
        /// </param>
        /// <returns>
        /// The updated <see cref="IServiceCollection"/>.
        /// </returns>
        public static IServiceCollection AddDistributedAzureBlobStorageCache(
            this IServiceCollection services,
            AzureBlobStorageCacheOptions options)
        {
            return AddDistributedAzureBlobStorageCache(
                services,
                options.ConnectionString,
                options.ContainerName);
        }

        /// <summary>
        /// Add Azure blob storage cache as an IDistributedCache to the service container.
        /// </summary>
        /// <param name="services">
        /// The <see cref="IServiceCollection"/>.
        /// </param>
        /// <param name="connectionString">
        /// The connection string of the storage account.
        /// </param>
        /// <param name="containerName">
        /// The name of the container to use. If the container doesn't exist it will be created.
        /// </param>
        /// <returns>
        /// The updated <see cref="IServiceCollection"/>.
        /// </returns>
        public static IServiceCollection AddDistributedAzureBlobStorageCache(
            this IServiceCollection services,
            string connectionString,
            string containerName)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException("connectionstring");

            if (String.IsNullOrWhiteSpace(containerName))
                throw new ArgumentNullException("containerName");

            services.Add(
                ServiceDescriptor.Singleton<IDistributedCache,
                AzureBlobStorageCache>(a => new AzureBlobStorageCache(connectionString, containerName)));

            return services;
        }

        /// <summary>
        /// Add Azure table storage cache as an IDistributedCache to the service container.
        /// </summary>
        /// <param name="services">
        /// The <see cref="IServiceCollection"/>.
        /// </param>
        /// <param name="options">
        /// The <see cref="AzureTableStorageCacheOptions"/>.
        /// </param>
        /// <returns>
        /// The updated <see cref="IServiceCollection"/>.
        /// </returns>
        public static IServiceCollection AddDistributedAzureTableStorageCache(
            this IServiceCollection services,
            AzureTableStorageCacheOptions options)
        {
            return AddDistributedAzureTableStorageCache(
                services,
                options.ConnectionString,
                options.TableName,
                options.PartitionKey);
        }

        /// <summary>
        /// Add Azure table storage cache as an IDistributedCache to the service container.
        /// </summary>
        /// <param name="services">
        /// The <see cref="IServiceCollection"/>.
        /// </param>
        /// <param name="connectionString">
        /// The connection string of the storage account.
        /// </param>
        /// <param name="tableName">
        /// The name of the table to use. If the table doesn't exist it will be created.
        /// </param>
        /// <param name="partitionKey">
        /// The partition key to use.
        /// </param>
        /// <returns>
        /// The updated <see cref="IServiceCollection"/>.
        /// </returns>
        public static IServiceCollection AddDistributedAzureTableStorageCache(
            this IServiceCollection services,
            string connectionString,
            string tableName,
            string partitionKey)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException("connectionstring");

            if (String.IsNullOrWhiteSpace(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (String.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException("tableName");

            services.Add(
                ServiceDescriptor.Singleton<IDistributedCache,
                AzureTableStorageCache>(a => new AzureTableStorageCache(connectionString, tableName, partitionKey)));

            return services;
        }
    }
}