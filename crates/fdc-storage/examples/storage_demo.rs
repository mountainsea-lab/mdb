//! Multi-Engine Storage System demonstration example

use fdc_storage::{
    backup::{BackupConfig, BackupManager},
    cache::{CacheManager, CachePolicy},
    compression::{CompressionAlgorithm, CompressionManager},
    config::StorageConfig,
    engine::{BatchOperation, StorageEngineFactory, StorageEngineType},
    index::{IndexConfig, IndexManager, IndexType},
    metrics::StorageMetrics,
    replication::{ReplicationConfig, ReplicationManager, ReplicationStrategy},
    shard::{ShardManager, ShardStrategy},
    tier::{StorageTier, TierConfig, TierManager},
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Multi-Engine Storage System Demo");
    println!("===================================");

    // 1. 演示存储配置
    demo_storage_config().await?;

    // 2. 演示存储引擎
    demo_storage_engines().await?;

    // 3. 演示层级管理
    demo_tier_management().await?;

    // 4. 演示数据分片
    demo_data_sharding().await?;

    // 5. 演示缓存管理
    demo_cache_management().await?;

    // 6. 演示压缩算法
    demo_compression().await?;

    // 7. 演示索引管理
    demo_index_management().await?;

    // 8. 演示备份恢复
    demo_backup_restore().await?;

    // 9. 演示复制管理
    demo_replication().await?;

    // 10. 演示存储指标
    demo_storage_metrics().await?;

    println!("\n✅ All multi-engine storage system demos completed successfully!");
    Ok(())
}

async fn demo_storage_config() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⚙️ Storage Configuration Demo");
    println!("-----------------------------");

    let config = StorageConfig::new()
        .with_data_dir("/tmp/fdc_storage".to_string())
        .with_shard_count(32)
        .with_replication(3)
        .with_compression(CompressionAlgorithm::Zstd)
        .with_backup_path("/tmp/backups".to_string());

    println!("📋 Storage Configuration:");
    println!("  Data Directory: {}", config.data_dir);
    println!("  Shard Count: {}", config.shard_count);
    println!("  Compression: {:?}", config.default_compression);
    println!(
        "  Replication: {} (factor: {})",
        config.enable_replication, config.replication_factor
    );
    println!(
        "  Backup: {} (path: {:?})",
        config.enable_backup, config.backup_path
    );

    // 验证配置
    config.validate()?;
    println!("✅ Configuration validation passed");

    // 显示启用的层级
    let enabled_tiers = config.enabled_tiers();
    println!("📊 Enabled tiers: {} layers", enabled_tiers.len());
    for tier in enabled_tiers {
        println!("  - {}", tier.name());
    }

    // 显示配置摘要
    println!("\n📄 Configuration Summary:");
    println!("{}", config.summary());

    Ok(())
}

async fn demo_storage_engines() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧 Storage Engines Demo");
    println!("-----------------------");

    // 演示内存引擎
    let mut memory_config = HashMap::new();
    memory_config.insert("max_size".to_string(), "1048576".to_string()); // 1MB

    let mut memory_engine =
        StorageEngineFactory::create_engine(StorageEngineType::Memory, memory_config).await?;

    println!("📊 Memory Engine:");
    println!("  Type: {}", memory_engine.engine_type());
    let caps = memory_engine.capabilities();
    println!("  Expected latency: {}μs", caps.expected_latency_us);
    println!(
        "  Expected throughput: {} ops/s",
        caps.expected_throughput_ops
    );
    println!("  Supports transactions: {}", caps.supports_transactions);
    println!("  Supports SQL: {}", caps.supports_sql);

    // 初始化引擎
    memory_engine.initialize().await?;

    // 基本操作演示
    let key = b"demo_key";
    let value = b"demo_value";

    memory_engine.put(key, value).await?;
    println!("✅ Put operation successful");

    let retrieved = memory_engine.get(key).await?;
    assert_eq!(retrieved, Some(value.to_vec()));
    println!("✅ Get operation successful");

    let exists = memory_engine.exists(key).await?;
    assert!(exists);
    println!("✅ Exists check successful");

    // 批量操作
    let batch_ops = vec![
        BatchOperation::Put {
            key: b"batch1".to_vec(),
            value: b"value1".to_vec(),
        },
        BatchOperation::Put {
            key: b"batch2".to_vec(),
            value: b"value2".to_vec(),
        },
        BatchOperation::Delete { key: key.to_vec() },
    ];

    memory_engine.batch(batch_ops).await?;
    println!("✅ Batch operations successful");

    // 扫描操作
    let scan_results = memory_engine.scan(None, None, Some(10)).await?;
    println!("📊 Scan results: {} items found", scan_results.len());

    // 获取统计信息
    let stats = memory_engine.stats().await?;
    println!("📈 Engine statistics:");
    println!("  Reads: {}", stats.reads);
    println!("  Writes: {}", stats.writes);
    println!("  Total operations: {}", stats.total_operations());

    // 健康检查
    let healthy = memory_engine.health_check().await?;
    println!(
        "💚 Engine health: {}",
        if healthy { "OK" } else { "FAILED" }
    );

    // 关闭引擎
    memory_engine.shutdown().await?;
    println!("✅ Engine shutdown successful");

    Ok(())
}

async fn demo_tier_management() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🏗️ Tier Management Demo");
    println!("-----------------------");

    let mut tier_manager = TierManager::new();

    // 配置各个层级
    let l1_config = TierConfig::new(StorageTier::L1)
        .with_max_size(1024 * 1024) // 1MB
        .with_migration_threshold(0.9);

    let l2_config = TierConfig::new(StorageTier::L2)
        .with_max_size(8 * 1024 * 1024) // 8MB
        .with_migration_threshold(0.8);

    tier_manager.add_tier(l1_config);
    tier_manager.add_tier(l2_config);

    println!("📊 Tier configuration:");
    println!("  L1 (Ultra-Hot): 1MB capacity, 90% migration threshold");
    println!("  L2 (Hot): 8MB capacity, 80% migration threshold");

    // 注意：由于其他存储引擎还未完全实现，这里只演示配置
    println!("✅ Tier management configuration completed");

    Ok(())
}

async fn demo_data_sharding() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔀 Data Sharding Demo");
    println!("--------------------");

    let shard_manager = ShardManager::new(16, ShardStrategy::Hash);

    println!("📊 Shard configuration:");
    println!("  Shard count: {}", shard_manager.get_shard_count());
    println!("  Strategy: Hash-based");

    // 演示分片键生成
    let test_keys: &[&[u8]] = &[
        b"user:1001",
        b"user:1002",
        b"order:2001",
        b"order:2002",
        b"product:3001",
    ];

    println!("\n🔑 Shard key distribution:");
    for key in test_keys {
        let shard_key = shard_manager.get_shard_key(key);
        println!(
            "  {:?} -> shard {}",
            String::from_utf8_lossy(key),
            shard_key.shard_id
        );
    }

    println!("✅ Data sharding demonstration completed");

    Ok(())
}

async fn demo_cache_management() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n💾 Cache Management Demo");
    println!("-----------------------");

    let mut cache_manager = CacheManager::new(CachePolicy::LRU, 3);

    println!("📊 Cache configuration:");
    println!("  Policy: LRU");
    println!("  Capacity: 3 items");

    // 缓存操作演示
    cache_manager.put(b"key1".to_vec(), b"value1".to_vec());
    cache_manager.put(b"key2".to_vec(), b"value2".to_vec());
    cache_manager.put(b"key3".to_vec(), b"value3".to_vec());

    println!("\n🔍 Cache operations:");

    // 缓存命中
    let hit = cache_manager.get(b"key1");
    println!("  Get key1: {}", if hit.is_some() { "HIT" } else { "MISS" });

    // 缓存未命中
    let miss = cache_manager.get(b"key4");
    println!(
        "  Get key4: {}",
        if miss.is_some() { "HIT" } else { "MISS" }
    );

    // 触发驱逐
    cache_manager.put(b"key4".to_vec(), b"value4".to_vec());
    println!("  Added key4 (should evict oldest)");

    // 获取统计信息
    let stats = cache_manager.stats();
    println!("\n📈 Cache statistics:");
    println!("  Hits: {}", stats.hits);
    println!("  Misses: {}", stats.misses);
    println!("  Hit rate: {:.2}%", stats.hit_rate() * 100.0);
    println!("  Evictions: {}", stats.evictions);
    println!("  Current size: {}", stats.size);

    println!("✅ Cache management demonstration completed");

    Ok(())
}

async fn demo_compression() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🗜️ Compression Demo");
    println!("------------------");

    let test_data = b"This is a test string that should compress well with repeated patterns. This is a test string that should compress well with repeated patterns.";

    // LZ4 压缩
    let lz4_manager = CompressionManager::new(CompressionAlgorithm::Lz4);
    let lz4_compressed = lz4_manager.compress(test_data)?;
    let lz4_decompressed = lz4_manager.decompress(&lz4_compressed)?;

    println!("📊 LZ4 Compression:");
    println!("  Original size: {} bytes", test_data.len());
    println!("  Compressed size: {} bytes", lz4_compressed.len());
    println!(
        "  Compression ratio: {:.2}%",
        (1.0 - lz4_compressed.len() as f64 / test_data.len() as f64) * 100.0
    );
    println!(
        "  Decompression successful: {}",
        lz4_decompressed == test_data
    );

    // Zstd 压缩
    let zstd_manager = CompressionManager::new(CompressionAlgorithm::Zstd);
    let zstd_compressed = zstd_manager.compress(test_data)?;
    let zstd_decompressed = zstd_manager.decompress(&zstd_compressed)?;

    println!("\n📊 Zstd Compression:");
    println!("  Original size: {} bytes", test_data.len());
    println!("  Compressed size: {} bytes", zstd_compressed.len());
    println!(
        "  Compression ratio: {:.2}%",
        (1.0 - zstd_compressed.len() as f64 / test_data.len() as f64) * 100.0
    );
    println!(
        "  Decompression successful: {}",
        zstd_decompressed == test_data
    );

    println!("✅ Compression demonstration completed");

    Ok(())
}

async fn demo_index_management() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📇 Index Management Demo");
    println!("-----------------------");

    let mut index_manager = IndexManager::new();

    // 创建索引
    let btree_index = IndexConfig {
        name: "user_id_index".to_string(),
        index_type: IndexType::BTree,
        columns: vec!["user_id".to_string()],
        unique: true,
    };

    let hash_index = IndexConfig {
        name: "email_index".to_string(),
        index_type: IndexType::Hash,
        columns: vec!["email".to_string()],
        unique: true,
    };

    index_manager.create_index(btree_index)?;
    index_manager.create_index(hash_index)?;

    println!("📊 Created indexes:");
    let indexes = index_manager.list_indexes();
    for index in indexes {
        println!(
            "  - {}: {:?} on {:?} (unique: {})",
            index.name, index.index_type, index.columns, index.unique
        );
    }

    // 删除索引
    index_manager.drop_index("email_index")?;
    println!("🗑️ Dropped email_index");

    let remaining_indexes = index_manager.list_indexes();
    println!("📊 Remaining indexes: {}", remaining_indexes.len());

    println!("✅ Index management demonstration completed");

    Ok(())
}

async fn demo_backup_restore() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n💾 Backup & Restore Demo");
    println!("------------------------");

    let backup_config = BackupConfig {
        path: "/tmp/fdc_backups".to_string(),
        compression: true,
        incremental: false,
    };

    let backup_manager = BackupManager::new(backup_config);

    println!("📊 Backup configuration:");
    println!("  Path: /tmp/fdc_backups");
    println!("  Compression: enabled");
    println!("  Type: full backup");

    // 创建备份（简化演示）
    let backup_id = backup_manager.create_backup().await?;
    println!("✅ Backup created with ID: {}", backup_id);

    println!("✅ Backup & restore demonstration completed");

    Ok(())
}

async fn demo_replication() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Replication Demo");
    println!("------------------");

    let replication_config = ReplicationConfig {
        factor: 3,
        strategy: ReplicationStrategy::Sync,
    };

    let replication_manager = ReplicationManager::new(replication_config);

    println!("📊 Replication configuration:");
    println!("  Factor: {}", replication_manager.get_config().factor);
    println!(
        "  Strategy: {:?}",
        replication_manager.get_config().strategy
    );

    println!("✅ Replication demonstration completed");

    Ok(())
}

async fn demo_storage_metrics() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Storage Metrics Demo");
    println!("----------------------");

    let mut metrics = StorageMetrics::new();

    // 模拟一些操作
    metrics.record_read(100);
    metrics.record_read(150);
    metrics.record_write(200);
    metrics.record_write(250);

    println!("📈 Storage metrics:");
    println!("  Reads: {}", metrics.reads);
    println!("  Writes: {}", metrics.writes);
    println!("  Total operations: {}", metrics.reads + metrics.writes);
    println!(
        "  Average read latency: {:.2}μs",
        metrics.avg_read_latency_us
    );
    println!(
        "  Average write latency: {:.2}μs",
        metrics.avg_write_latency_us
    );

    if let Some(uptime) = metrics.uptime() {
        println!("  Uptime: {:.2}s", uptime.as_secs_f64());
        println!(
            "  Operations per second: {:.2}",
            metrics.operations_per_second()
        );
    }

    println!("✅ Storage metrics demonstration completed");

    Ok(())
}
