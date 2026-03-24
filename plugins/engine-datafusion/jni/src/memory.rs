/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use datafusion::common::DataFusionError;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;


/// Wrapper around MonitoredMemoryPool providing access to memory monitoring capabilities.
#[derive(Debug)]
pub struct CustomMemoryPool {
    memory_pool: Arc<MonitoredMemoryPool>
}

impl CustomMemoryPool {
    pub fn new(memory_pool: Arc<MonitoredMemoryPool>) -> Self {
        Self { memory_pool }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.memory_pool.get_monitor()
    }

    pub fn get_memory_pool(&self) -> Arc<dyn MemoryPool> {
        self.memory_pool.clone()
    }
}

/// Tracks current and peak memory usage atomically.
#[derive(Debug, Default)]
pub(crate) struct Monitor {
    pub(crate) value: AtomicUsize,
    pub(crate) max: AtomicUsize,
}

impl Monitor {
    pub(crate) fn max(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }

    fn grow(&self, amount: usize) {
        let old = self.value.fetch_add(amount, Ordering::Relaxed);
        self.max.fetch_max(old + amount, Ordering::Relaxed);
    }

    fn shrink(&self, amount: usize) {
        self.value.fetch_sub(amount, Ordering::Relaxed);
    }

    pub(crate) fn get_current_val(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// A MemoryPool that supports changing its limit at runtime.
///
/// Unlike DataFusion's GreedyMemoryPool (which stores pool_size as a plain usize
/// set once in the constructor with no setter), DynamicLimitPool uses an AtomicUsize
/// for the limit, allowing it to be changed at any time via the shared limit handle.
///
/// Behavior:
/// - Increasing the limit takes effect immediately for new allocations.
/// - Decreasing the limit takes effect for new allocations only.
///   Existing reservations that exceed the new limit are NOT reclaimed.
///   They remain until the consumer frees them (e.g., query completes).
#[derive(Debug)]
pub struct DynamicLimitPool {
    /// Current memory usage, tracked atomically.
    used: AtomicUsize,
    /// The dynamic limit that can be changed at runtime.
    /// Shared via Arc so the limit can be changed externally.
    dynamic_limit: Arc<AtomicUsize>,
    /// Debug: delay in milliseconds to inject into each try_grow call.
    /// When non-zero, every try_grow sleeps for this duration BEFORE checking the limit.
    /// This artificially slows down query execution so you can change the limit mid-query.
    /// Set to 0 (default) for production use.
    debug_delay_ms: Arc<AtomicUsize>,
}

/// Handle to change the pool limit at runtime.
/// Can be stored separately from the pool itself.
#[derive(Debug, Clone)]
pub struct DynamicLimitHandle {
    limit: Arc<AtomicUsize>,
    debug_delay_ms: Arc<AtomicUsize>,
}

impl DynamicLimitHandle {
    /// Change the pool limit at runtime.
    pub fn set_limit(&self, new_limit: usize) {
        self.limit.store(new_limit, Ordering::SeqCst);
    }

    /// Get the current limit.
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::SeqCst)
    }

    /// Set debug delay in milliseconds for try_grow calls.
    /// When non-zero, every try_grow sleeps for this duration before checking the limit.
    /// Use this to slow down queries so you can change the limit mid-execution.
    pub fn set_debug_delay_ms(&self, ms: usize) {
        self.debug_delay_ms.store(ms, Ordering::SeqCst);
    }

    /// Get the current debug delay in milliseconds.
    pub fn debug_delay_ms(&self) -> usize {
        self.debug_delay_ms.load(Ordering::SeqCst)
    }
}

impl DynamicLimitPool {
    /// Create a new pool with the given initial limit.
    /// Returns the pool and a handle to change the limit.
    pub fn new(initial_limit: usize) -> (Self, DynamicLimitHandle) {
        let limit = Arc::new(AtomicUsize::new(initial_limit));
        let delay = Arc::new(AtomicUsize::new(0));
        let handle = DynamicLimitHandle {
            limit: limit.clone(),
            debug_delay_ms: delay.clone(),
        };
        let pool = Self {
            used: AtomicUsize::new(0),
            dynamic_limit: limit,
            debug_delay_ms: delay,
        };
        (pool, handle)
    }

    /// Get the current limit.
    pub fn limit(&self) -> usize {
        self.dynamic_limit.load(Ordering::SeqCst)
    }
}

impl MemoryPool for DynamicLimitPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        // Debug: sleep before checking limit to slow down query execution
        let delay_ms = self.debug_delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            thread::sleep(Duration::from_millis(delay_ms as u64));
        }

        let limit = self.dynamic_limit.load(Ordering::SeqCst);
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let new_used = used + additional;
                (new_used <= limit).then_some(new_used)
            })
            .map_err(|used| {
                DataFusionError::ResourcesExhausted(format!(
                    "Failed to allocate additional {} for {} with {} already allocated \
                     for this reservation - {} remain available for the total pool (dynamic limit: {})",
                    additional,
                    reservation.consumer().name(),
                    reservation.size(),
                    limit.saturating_sub(used),
                    limit,
                ))
            })?;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// MemoryPool implementation that wraps another pool and tracks memory usage via Monitor.
#[derive(Debug)]
pub struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<Monitor>,
}

impl MonitoredMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<Monitor>) -> Self {
        Self { inner, monitor }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.monitor.clone()
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn register(&self, _consumer: &MemoryConsumer) {
        self.inner.register(_consumer)
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        self.inner.unregister(_consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.monitor.grow(additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.monitor.shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.monitor.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}
