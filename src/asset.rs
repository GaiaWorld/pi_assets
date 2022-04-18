//! Asset 资产


use futures::io;
use pi_any::{BoxAny, impl_downcast_box};
use pi_hash::XHashMap;
use slotmap::{DefaultKey, Key};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::{collections::hash_map, any::TypeId};
use std::hash::Hash;
use std::fmt::Debug;
use std::mem::replace;
use pi_cache::{Cache, Data};
use futures::future::BoxFuture;

/// 资产定义
pub trait Asset {
    /// 关联键的类型
    type Key: Hash + Eq + Clone + Debug;
    /// 资产的关联键
    // fn key(&self) -> Self::Key;
    /// 资产的大小
    fn size(&self) -> usize;
}

/// 回收器定义， 在多线程环境中，一般采用线程局部存储来获得回收队列
pub trait Garbageer<T: Asset> {
    /// 在进入锁前调用
    fn start(&self);
    /// 在锁中调用
    fn garbage(&self, k: T::Key, v: T);
    /// 退出锁后调用
    fn end(&self);
}

/// 加载器定义
pub trait AssetLoader {
    type A: Asset;
    type Param;
    
    fn load(&self, p: Self::Param) -> BoxFuture<io::Result<Self::A>>;

}

/// 资产表
pub(crate) struct AssetMap<T: Asset> {
    /// 正在使用的资产表
    map: XHashMap<<T as Asset>::Key, ArcDrop<T>>,
    /// 没有被使用的资产缓存表
    cache: Cache<T::Key, Item<T>>,
    /// 正在使用的资产的大小 TODO 优化到外边的AtomicSize
    size: usize,
}

/// 资产条目
struct Item<A: Asset>(A, usize);
impl<A: Asset> Data for Item<A> {
    fn size(&self) -> usize {
        self.0.size()
    }
    fn timeout(&self) -> usize {
        self.1
    }
}

/// 可拷贝回收的资产句柄
pub type ArcDrop<T> = Arc<Droper<T>>;
pub struct Droper<T: Asset> {
    data: Option<T>,
    asset_map: *const Mutex<AssetMap<T>>,
}
impl<T: Asset> Deref for Droper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self.data {
            Some(d) => d,
            _ => panic!("called `deref()` on a `None` value"),
        }
    }
}
impl<T: Asset> Drop for Droper<T> {
    fn drop(&mut self) {
        let d = unsafe {self.data.take().unwrap_unchecked()};
        todo!()
    }
}

