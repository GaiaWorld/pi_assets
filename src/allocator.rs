//! 多个资产管理器的容量分配器

use pi_async_rt::prelude::AsyncRuntime;

use pi_share::Share;
use pi_time::now_millisecond;
use serde::{Deserialize, Serialize};

use crate::asset::*;
use crate::homogeneous::{Garbageer as Gar, HomogeneousMgr};
use crate::mgr::AssetMgr;

pub trait Collect: Send + Sync {
    /// 设置整理器的容量
    fn set_capacity(&self, capacity: usize);
    /// 获得整理器的大小(正在使用和未使用的资源大小)
    fn size(&self) -> usize;
    /// 获得整理器正在使用的大小
    fn using_size(&self) -> usize;
    /// 超时整理方法， 清理最小容量外的超时资产
    fn timeout_collect(&self, capacity: usize, now: u64);
    /// 超量整理方法， 清理超出容量的资产
    fn capacity_collect(&self, capacity: usize);
	/// 资源统计
	fn account(&self) -> AssetMgrAccount;
}


// 默认满容量的比例
const FULL: f32 = 0.9;

/// 容量分配器
pub struct Allocator {
    /// 资产管理器列表
    vec: Vec<Item>,
    /// 最大容量
    total_capacity: usize,
    /// 统计总计的最小容量
    min_capacity: usize,
    /// 统计总计的权重
    total_weight: usize,
    /// 整理时用的临时满容量数组
    temp_full: Vec<usize>,
    /// 整理时用的临时超出数组
    temp_overflow: Vec<(usize, usize)>,
}

impl Allocator {

    /// 用指定的最大内存容量创建资产管理器
    pub fn new(total_capacity: usize) -> Self {
        Allocator {
            vec: Vec::new(),
            total_capacity,
            min_capacity: 0,
            total_weight: 0,
            temp_full: Vec::new(),
            temp_overflow: Vec::new(),
        }
    }
    /// 注册可被整理的管理器，要求必须单线程注册
    pub fn register(&mut self, mgr: Share<dyn Collect>, min_capacity: usize, weight: usize) {
        self.min_capacity += min_capacity;
        self.total_weight += weight;
        let item = Item {
            mgr,
            min_capacity,
            weight,
            weight_capacity: 0,
            capacity: 0,
        };
        self.vec.push(item);
    }
    /// 获得总大小
    pub fn size(&self) -> usize {
        let mut r = 0;
        for v in self.vec.iter() {
            r += v.mgr.size();
        }
        r
    }
    /// 获得指定组别的总容量
    pub fn total_capacity(&self) -> usize {
        self.total_capacity
    }
    /// 获得指定组别的全部资产缓存的累计最小容量
    pub fn min_capacity(&self) -> usize {
        self.min_capacity
    }
    /// 整理方法， 清理超时和超容量的资源。
    /// 分配器有总内存容量， 按权重分给其下的Mgr。
    /// 如果总容量有空闲， 则按权重提高那些满的Mgr的容量
    pub fn collect(&mut self, _now: u64) {
        let now = pi_time::now_millisecond();
        // 如果有未计算的权重容量， 表示需要重新计算权重
        // if self.vec.len() > 0 && self.vec[self.vec.len() - 1].weight_capacity == 0 {
            // // 计算理论容量
            // let c1 = self.total_weight - self.min_capacity;
            // // 计算实际容量
            // let c2 = if self.total_capacity > self.min_capacity {
            //     self.total_capacity - self.min_capacity
            // } else {
            //     0
            // };
            // if c1 != 0 && c1 > usize::MAX / c1 {
            //     let f = c2 as f64 / c1 as f64;
            //     // 计算每个资产分组缓存队列的权重容量
            //     for i in self.vec.iter_mut() {
            //         i.weight_capacity = i.min_capacity + ((i.weight - i.min_capacity) as f64 * f) as usize;
            //         i.capacity = i.weight_capacity;
            //     }
            // }else{
            //     // 计算每个资产分组缓存队列的权重容量
            //     for i in self.vec.iter_mut() {
            //         i.weight_capacity = i.min_capacity + (i.weight - i.min_capacity) * c2 / c1;
            //         i.capacity = i.weight_capacity;
            //     }
            // }

            // 计算实际容量
            let mut using_size = 0;
            for i in 0..self.vec.len() {
                let item = &mut self.vec[i];
                using_size += item.mgr.using_size().max(item.min_capacity);
            }

            let c2 = if self.total_capacity > using_size {
                self.total_capacity - using_size
            } else {
                0
            };

            for i in 0..self.vec.len() {
                let item: &mut Item = &mut self.vec[i];
                item.weight_capacity = item.mgr.using_size().max(item.min_capacity) + (item.weight as f32/self.total_weight as f32 * c2 as f32) as usize;
                item.capacity = item.capacity.max(item.weight_capacity);

                // if i == 13 {
                //     log::error!("item x: {}, {:?}, {:?}", i, item.capacity, item.weight_capacity);
                // }

                // log::error!("init1====={:?}", (i.weight_capacity as f32/1024.0/1024.0, i.capacity as f32/1024.0/1024.0, i.min_capacity as f32/1024.0/1024.0, i.weight, self.total_weight, c2 as f32/1024.0/1024.0, (i.weight * c2/self.total_weight) as f32/1024.0/1024.0, (i.min_capacity + (i.weight/self.total_weight * c2)) as f32/1024.0/1024.0), );
            }

            // log::error!("init====={:?}", (self.total_weight, self.total_capacity, self.min_capacity, c2));
            
        // }
        // 最小容量下，仅进行最小容量清理操作
        if self.total_capacity <= self.min_capacity {
            for i in self.vec.iter() {
                i.mgr.timeout_collect(i.min_capacity, now);
                i.mgr.capacity_collect(i.min_capacity);
            }
            return;
        }
        // 超过权重的容量和， 溢出容量
        let mut overflow_size = 0;
        // 空闲容量
        let mut free_size = 0;
        // 满的容量和
        let mut full_size = 0;
        // 先用超时整理腾出空间，统计每种资源的占用
        for i in 0..self.vec.len() {
            let item = &mut self.vec[i];
            item.mgr.timeout_collect(item.min_capacity, now);
            let size = item.mgr.size();
            // println!("item i: {}, {:?}, {:?}", i, size, item.weight_capacity);
            // if i == 13 {
            //     // log::error!("item i: {}, {:?}, {:?}", i, size, item.weight_capacity);
            // }
            if size <= item.weight_capacity {
                // 如果当前内存小于权重容量，将多余容量放到free_size上
                free_size += item.weight_capacity - size;
                // 将当前容量设置为权重容量
                item.capacity = item.weight_capacity;
            } else {
                // 如果当前内存大于权重容量，则记录该条目，并累计overflow权重
                let overflow = size - item.weight_capacity;
                self.temp_overflow.push((i, overflow));
                // 并将超出大小放到overflow_size上
                overflow_size += overflow;
            }
            // 如果条目已经满了，则记录该条目，并累计capacity权重
            if size as f32 > item.capacity as f32 * FULL {
                self.temp_full.push(i);
                full_size += item.capacity;
            }
        }
        // println!("free_size : {}, {:?}, {:?}, {:?}", free_size, overflow_size, self.temp_full.len(), self.temp_overflow.len());
        if free_size > overflow_size {
            let size = free_size - overflow_size;
            // 空闲比溢出的多，表示需要扩张容量，将满的条目按capacity权重进行扩大
			
            for index in &self.temp_full {
                let item = &mut self.vec[*index];
                let fix = (size as f64 * (item.capacity as f64 / full_size as f64)) as usize;
                item.capacity += fix;
                // if *index == 13 {
                //     item.capacity = 0;
                //     // log::error!("index i: {:?}", (item.capacity, fix, size, full_size));
                // }
                // log::error!("free====={:?}", (index, self.total_weight, item.capacity, fix));
            }
        } else if free_size < overflow_size {
            let size = overflow_size - free_size;
            // 空闲比溢出的少，表示需要收缩，将溢出的条目按overflow权重进行缩小
            for (index, overflow) in &self.temp_overflow {
                let item = &mut self.vec[*index];
                let fix = (size as f64 * (*overflow as f64 / overflow_size as f64)) as usize;
                item.capacity = item.weight_capacity + *overflow - fix;
                // if *index == 13 {
                //     // log::error!("index1 i: {:?}", (item.capacity, fix, item.weight_capacity, *overflow, size));
                // }
                // log::error!("overflow====={:?}", (index, self.total_weight, item.capacity, fix, *overflow));
            }
        }
        self.temp_full.clear();
        self.temp_overflow.clear();
        // 超量整理
        for i in self.vec.iter() {
            // println!("size:{:?}, capacity:{:?}", i.mgr.size(), i.capacity);
            i.mgr.set_capacity(i.capacity);
            i.mgr.capacity_collect(i.capacity);
        }
    }
    /// 用指定的间隔时间进行自动整理
    pub fn auto_collect<RT>(mut self, rt: RT, interval: usize)
    where
        RT: AsyncRuntime
    {
        let rt1 = rt.clone();
        let _ = rt.spawn(async move {
            loop {
                rt1.timeout(interval).await;
                self.collect(now_millisecond());
            }
        });
    }

	/// 统计资产管理器的缓冲情况
	pub fn account(&self) -> AssetsAccount {
		let mut r = AssetsAccount::default();
		for item in self.vec.iter() {
			let mut account = item.mgr.account();
			account.min_capacity = item.min_capacity as f32 / 1024.0;
			account.weight = item.weight;
			account.weight_capacity = item.weight_capacity as f32 / 1024.0;
			account.capacity = item.capacity as f32 / 1024.0;
			r.cur_total_size += account.used_size;
			r.cur_total_size += account.unused_size;
			r.list.push(account);
		}
        r.total_capacity = self.total_capacity as f32 / 1024.0;
        r.min_capacity = self.min_capacity as f32 / 1024.0;
        r.total_weight = self.total_weight as f32;
        r
	}

}
struct Item {
    mgr: Share<dyn Collect>,
    min_capacity: usize,
    weight: usize,
    weight_capacity: usize,
    capacity: usize,
}

/// 每资产管理器信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetMgrAccount {
	pub name: String,

    pub size: f32, // 单位 k
	pub used_size: f32, // 单位 k
    pub unused_size: f32, // 单位 k
	pub used: Vec<AssetInfo>,
	pub unused: Vec<AssetInfo>,

    pub min_capacity: f32, // 单位 k
    pub weight: usize,
    pub weight_capacity: f32, // 单位 k
    pub capacity: f32, // 单位 k
    pub ty: u32,
    pub visit_count: usize,
    pub hit_rate: f32,
    pub timeout: usize, // 单位ms
}

/// 每资产信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetInfo {
	/// 资源名称
	pub name: String, 
	/// 资源大小
	pub size: f32, // 单位 k
	// 剩余多长时间超时
	pub remain_timeout: u64, 
}

/// 资产统计信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetsAccount {
    pub cur_total_size: f32, // 当前大小 单位 k
	pub list: Vec<AssetMgrAccount>,
    pub total_capacity: f32, // 总容量 单位 k
    pub total_weight: f32, // 总容量 单位 k
    pub min_capacity: f32, // 总容量 单位 k
}

impl<A: Asset, G: Garbageer<A>> Collect for AssetMgr<A, G> {
    fn set_capacity(&self, capacity: usize) {
        self.set_capacity(capacity)
    }
    fn size(&self) -> usize {
        self.size()
    }
    fn using_size(&self) -> usize {
        self.using_size()
    }
    fn timeout_collect(&self, capacity: usize, now: u64) {
        self.timeout_collect(capacity, now)
    }
    fn capacity_collect(&self, capacity: usize) {
        self.capacity_collect(capacity)
    }
	/// 资源大小
	fn account(&self) -> AssetMgrAccount {
		self.account()
	}
}
impl<V: Size, G: Gar<V>> Collect for HomogeneousMgr<V, G> {
    fn set_capacity(&self, capacity: usize) {
        self.set_capacity(capacity)
    }
    fn size(&self) -> usize {
        self.size()
    }
    fn using_size(&self) -> usize {
        0
    }
    fn timeout_collect(&self, capacity: usize, now: u64) {
        self.timeout_collect(capacity, now)
    }
    fn capacity_collect(&self, capacity: usize) {
        self.capacity_collect(capacity)
    }
	/// 资源大小
	fn account(&self) -> AssetMgrAccount {
		self.account()
	}
}

#[cfg(test)]
mod test_mod {
    use crate::{allocator::Allocator, asset::*, mgr::*};
    use pi_async_rt::prelude::{multi_thread::MultiTaskRuntimeBuilder, AsyncRuntime};
    use std::time::Duration;

    #[derive(Debug, Eq, PartialEq)]
    struct R1(usize, usize, usize);

    impl Asset for R1 {
        type Key = usize;
    }

	impl Size for R1 {
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    struct R2(usize, usize, usize);

    impl Asset for R2 {
        type Key = usize;
    }

	impl Size for R2 {
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    struct R3(usize, usize, usize);

    impl Asset for R3 {
        type Key = usize;
    }

	impl Size for R3 {
        /// 资源的大小
        fn size(&self) -> usize {
            self.1
        }
    }

    #[test]
    pub fn test() {
        let pool = MultiTaskRuntimeBuilder::default();
        let rt0 = pool.build();
        let _ = rt0.spawn(async move {
            let mgr = AssetMgr::<R1, _>::new(GarbageEmpty(), false, 1024 * 1024, 3 * 60 * 1000);
            let m = AssetMgr::<R2, _>::new(GarbageEmpty(), false, 1024 * 1024, 3 * 60 * 1000);
            let mm = AssetMgr::<R3, _>::new(GarbageEmpty(), false, 1024 * 1024, 3 * 60 * 1000);
            let mut all = Allocator::new(100);
            all.register(mgr.clone(), 1, 100);
            all.register(m.clone(), 1, 100);
            all.register(mm.clone(), 1, 50);
            mgr.cache(1, R1(1, 1, 0));
            mgr.cache(10, R1(10, 10, 0));
            //mgr.cache(100, R1(100, 100, 0));
            m.cache(20, R2(20, 20, 0));
            m.cache(19, R2(19, 19, 0));
            println!("mgr_info: {:?}", mgr.info());
            all.collect(u64::MAX);
        });
        std::thread::sleep(Duration::from_millis(5000));
    }
}
