use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, Client, ScriptInvocation};
use crate::error::TurboPulseError;

#[derive(Clone)]
pub enum RedisBackend {
    Standalone(ConnectionManager),
    Cluster(ClusterConnection),
}

impl RedisBackend {
    pub async fn new(redis_url: &str) -> Result<Self, TurboPulseError> {
        let is_cluster = redis_url.contains(',')
            || redis_url.starts_with("redis+cluster://")
            || redis_url.contains("cluster=true");

        if is_cluster {
            let clean_url = redis_url
                .trim_start_matches("redis+cluster://")
                .trim();
            let nodes: Vec<&str> = if clean_url.contains(',') {
                clean_url.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect()
            } else {
                vec![clean_url]
            };

            let client = ClusterClient::new(nodes).map_err(TurboPulseError::Redis)?;
            let conn = client.get_async_connection().await.map_err(TurboPulseError::Redis)?;
            Ok(RedisBackend::Cluster(conn))
        } else {
            let client = Client::open(redis_url).map_err(TurboPulseError::Redis)?;
            let manager = ConnectionManager::new(client).await.map_err(TurboPulseError::Redis)?;
            Ok(RedisBackend::Standalone(manager))
        }
    }

    pub async fn invoke_script<T: redis::FromRedisValue>(
        &self,
        inv: &ScriptInvocation<'_>,
    ) -> Result<T, TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                inv.invoke_async(&mut conn).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                inv.invoke_async(&mut conn).await.map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.get(key).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.get(key).await.map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn set(&self, key: &str, val: &str) -> Result<(), TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.set::<_, _, ()>(key, val).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.set::<_, _, ()>(key, val).await.map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn del(&self, key: &str) -> Result<(), TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.del::<_, ()>(key).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.del::<_, ()>(key).await.map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn zcard(&self, key: &str) -> Result<u32, TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                let count: u32 = conn.zcard(key).await.map_err(TurboPulseError::Redis)?;
                Ok(count)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                let count: u32 = conn.zcard(key).await.map_err(TurboPulseError::Redis)?;
                Ok(count)
            }
        }
    }

    pub async fn zrem(&self, key: &str, member: &str) -> Result<i64, TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.zrem(key, member).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.zrem(key, member).await.map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn zadd(&self, key: &str, member: &str, score: i64) -> Result<(), TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.zadd::<_, _, _, ()>(key, member, score).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.zadd::<_, _, _, ()>(key, member, score).await.map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn zrangebyscore_limit(
        &self,
        key: &str,
        min: u64,
        max: u64,
        offset: isize,
        count: isize,
    ) -> Result<Vec<String>, TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                let res: Vec<String> = conn
                    .zrangebyscore_limit(key, min, max, offset, count)
                    .await
                    .map_err(TurboPulseError::Redis)?;
                Ok(res)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                let res: Vec<String> = conn
                    .zrangebyscore_limit(key, min, max, offset, count)
                    .await
                    .map_err(TurboPulseError::Redis)?;
                Ok(res)
            }
        }
    }

    pub async fn hset(&self, key: &str, field: &str, val: &str) -> Result<(), TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.hset::<_, _, _, ()>(key, field, val)
                    .await
                    .map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.hset::<_, _, _, ()>(key, field, val)
                    .await
                    .map_err(TurboPulseError::Redis)
            }
        }
    }

    pub async fn incr(&self, key: &str) -> Result<i64, TurboPulseError> {
        match self {
            RedisBackend::Standalone(m) => {
                let mut conn = m.clone();
                conn.incr(key, 1).await.map_err(TurboPulseError::Redis)
            }
            RedisBackend::Cluster(c) => {
                let mut conn = c.clone();
                conn.incr(key, 1).await.map_err(TurboPulseError::Redis)
            }
        }
    }
}

