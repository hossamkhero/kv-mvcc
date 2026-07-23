use arc_swap::ArcSwap;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
};
use tokio::sync::RwLock;

/// Common contract implemented by every storage backend.
///
/// The returned futures are explicitly `Send` so generic benchmark workers
/// can execute them inside `tokio::spawn`.
pub trait Store: Send + Sync {
    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Future<Output = Option<String>> + Send + 'a;

    fn add<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> impl Future<Output = bool> + Send + 'a;

    fn update<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> impl Future<Output = bool> + Send + 'a;

    fn remove<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Future<Output = bool> + Send + 'a;
}

/* -------------------------------------------------------------------------- */
/*                       Plain globally locked BTreeMap                        */
/* -------------------------------------------------------------------------- */

pub struct LockedBTree {
    map: RwLock<BTreeMap<String, String>>,
}

impl LockedBTree {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Default for LockedBTree {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for LockedBTree {
    async fn get<'a>(&'a self, key: &'a str) -> Option<String> {
        self.map.read().await.get(key).cloned()
    }

    async fn add<'a>(&'a self, key: &'a str, value: &'a str) -> bool {
        let mut map = self.map.write().await;

        if map.contains_key(key) {
            return false;
        }

        map.insert(key.to_owned(), value.to_owned());
        true
    }

    async fn update<'a>(&'a self, key: &'a str, value: &'a str) -> bool {
        let mut map = self.map.write().await;

        let Some(current) = map.get_mut(key) else {
            return false;
        };

        *current = value.to_owned();
        true
    }

    async fn remove<'a>(&'a self, key: &'a str) -> bool {
        self.map.write().await.remove(key).is_some()
    }
}

/* -------------------------------------------------------------------------- */
/*                         BTreeMap with ArcSwap cells                         */
/* -------------------------------------------------------------------------- */

pub struct ArcSwapBTree {
    map: RwLock<BTreeMap<String, ArcSwap<String>>>,
}

impl ArcSwapBTree {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Default for ArcSwapBTree {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for ArcSwapBTree {
    async fn get<'a>(&'a self, key: &'a str) -> Option<String> {
        let map = self.map.read().await;

        map.get(key).map(|cell| {
            let value = cell.load();
            (**value).clone()
        })
    }

    async fn add<'a>(&'a self, key: &'a str, value: &'a str) -> bool {
        let mut map = self.map.write().await;

        if map.contains_key(key) {
            return false;
        }

        map.insert(
            key.to_owned(),
            ArcSwap::from_pointee(value.to_owned()),
        );

        true
    }

    async fn update<'a>(&'a self, key: &'a str, value: &'a str) -> bool {
        let map = self.map.read().await;

        let Some(cell) = map.get(key) else {
            return false;
        };

        cell.store(Arc::new(value.to_owned()));
        true
    }

    async fn remove<'a>(&'a self, key: &'a str) -> bool {
        self.map.write().await.remove(key).is_some()
    }
}
