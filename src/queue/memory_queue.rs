use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::config::BackoffType;
use crate::error::TurboPulseError;
use crate::queue::job::{JobCounts, JobRecord, JobState};
use crate::utils::time::{calculate_backoff, current_time_ms};

/// Element in the waiting priority queue
#[derive(Debug, Eq, PartialEq)]
struct PrioritizedJob {
    priority: i32,
    sequence: u64,
    job_id: String,
}

impl Ord for PrioritizedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first; if equal, lower sequence (earlier) first
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for PrioritizedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Element in the delayed min-heap (scheduled jobs / retries)
#[derive(Debug, Eq, PartialEq)]
struct DelayedJob {
    delay_until_ms: u64,
    job_id: String,
}

impl Ord for DelayedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: lowest delay_until_ms comes first
        other.delay_until_ms.cmp(&self.delay_until_ms)
    }
}

impl PartialOrd for DelayedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct QueueState {
    waiting: Mutex<BinaryHeap<PrioritizedJob>>,
    delayed: Mutex<BinaryHeap<DelayedJob>>,
    active_ids: DashMap<String, u64>,
    completed_ids: DashMap<String, u64>,
    failed_ids: DashMap<String, u64>,
    sequence_counter: AtomicU64,
    paused: AtomicBool,
}

impl QueueState {
    fn new() -> Self {
        Self {
            waiting: Mutex::new(BinaryHeap::new()),
            delayed: Mutex::new(BinaryHeap::new()),
            active_ids: DashMap::new(),
            completed_ids: DashMap::new(),
            failed_ids: DashMap::new(),
            sequence_counter: AtomicU64::new(0),
            paused: AtomicBool::new(false),
        }
    }
}

/// Production-grade In-Memory Queue engine.
/// Provides full queue feature parity (priority, delays, retries with exponential backoff, progress)
/// completely outside the V8 heap in native memory.
pub struct InMemoryQueue {
    queues: DashMap<String, Arc<QueueState>>,
    jobs: DashMap<String, JobRecord>,
    parent_pending: DashMap<String, AtomicU32>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
            jobs: DashMap::new(),
            parent_pending: DashMap::new(),
        }
    }

    pub fn register_parent_children(&self, parent_id: &str, count: u32) {
        self.parent_pending.insert(parent_id.to_string(), AtomicU32::new(count));
    }

    fn get_or_create_queue(&self, queue_name: &str) -> Arc<QueueState> {
        self.queues
            .entry(queue_name.to_string())
            .or_insert_with(|| Arc::new(QueueState::new()))
            .clone()
    }

    pub fn push_job(&self, job: JobRecord) -> Result<(), TurboPulseError> {
        let q = self.get_or_create_queue(&job.queue_name);
        let job_id = job.id.clone();
        let now = current_time_ms();

        if job.delay_until_ms > now {
            let mut delayed_guard = q.delayed.lock();
            delayed_guard.push(DelayedJob {
                delay_until_ms: job.delay_until_ms,
                job_id: job_id.clone(),
            });
        } else {
            let raw_seq = q.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
            let seq = if job.config.lifo.unwrap_or(false) {
                // For LIFO, invert the sequence so it ranks higher in the heap for the same priority
                u64::MAX - raw_seq
            } else {
                raw_seq
            };
            let mut waiting_guard = q.waiting.lock();
            waiting_guard.push(PrioritizedJob {
                priority: job.config.priority.unwrap_or(0),
                sequence: seq,
                job_id: job_id.clone(),
            });
        }

        self.jobs.insert(job_id, job);
        Ok(())
    }

    pub fn push_jobs_bulk(&self, jobs: Vec<JobRecord>) -> Result<Vec<String>, TurboPulseError> {
        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            let id = job.id.clone();
            self.push_job(job)?;
            ids.push(id);
        }
        Ok(ids)
    }

    pub fn fetch_next_batch(&self, queue_name: &str, count: usize) -> Result<Vec<JobRecord>, TurboPulseError> {
        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(j) = self.fetch_next_job(queue_name)? {
                list.push(j);
            } else {
                break;
            }
        }
        Ok(list)
    }

    /// Moves any matured delayed jobs into the waiting queue and pops the next waiting job
    pub fn fetch_next_job(&self, queue_name: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        let q = self.get_or_create_queue(queue_name);

        if q.paused.load(AtomicOrdering::Relaxed) {
            return Ok(None);
        }

        let now = current_time_ms();

        // 1. Promote matured delayed jobs into waiting
        {
            let mut delayed_guard = q.delayed.lock();
            let mut waiting_guard = q.waiting.lock();

            while let Some(top) = delayed_guard.peek() {
                if top.delay_until_ms <= now {
                    let delayed_item = delayed_guard.pop().unwrap();
                    if let Some(mut job) = self.jobs.get_mut(&delayed_item.job_id) {
                        if job.state == JobState::Delayed {
                            job.state = JobState::Waiting;
                            let seq = q.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
                            waiting_guard.push(PrioritizedJob {
                                priority: job.config.priority.unwrap_or(0),
                                sequence: seq,
                                job_id: delayed_item.job_id,
                            });
                        }
                    }
                } else {
                    break;
                }
            }
        }

        // 2. Pop next job from waiting queue
        let mut waiting_guard = q.waiting.lock();
        while let Some(p_job) = waiting_guard.pop() {
            if let Some(mut job) = self.jobs.get_mut(&p_job.job_id) {
                if job.state == JobState::Waiting {
                    job.state = JobState::Active;
                    job.attempts_made += 1;
                    job.processed_at = Some(now);

                    q.active_ids.insert(p_job.job_id.clone(), now);
                    return Ok(Some(job.clone()));
                }
            }
        }

        Ok(None)
    }

    pub fn complete_job(
        &self,
        queue_name: &str,
        job_id: &str,
        return_value: Option<Vec<u8>>,
    ) -> Result<(), TurboPulseError> {
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();

        q.active_ids.remove(job_id);

        let mut repeat_to_schedule: Option<JobRecord> = None;
        let mut parent_to_promote: Option<String> = None;
        let mut remove_now = false;

        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.state = JobState::Completed;
            job.finished_at = Some(now);
            job.return_value = return_value;
            job.progress = 100;

            if job.config.remove_on_complete.unwrap_or(false) {
                remove_now = true;
            }

            // Check repeat options (repeat_every_ms, repeat_limit)
            if let Some(every_ms) = job.config.repeat_every_ms {
                let limit = job.config.repeat_limit.unwrap_or(u32::MAX);
                if job.attempts_made < limit {
                    let mut next_job = job.clone();
                    next_job.id = format!("{}:{}", job.id, now);
                    next_job.state = JobState::Delayed;
                    next_job.delay_until_ms = now + every_ms as u64;
                    next_job.created_at = now;
                    next_job.processed_at = None;
                    next_job.finished_at = None;
                    next_job.attempts_made = 0;
                    next_job.stalled_count = 0;
                    repeat_to_schedule = Some(next_job);
                }
            }

            // Check parent-child DAG dependency
            if let Some(ref p_id) = job.config.parent_id {
                if let Some(entry) = self.parent_pending.get(p_id) {
                    let prev = entry.value().fetch_sub(1, AtomicOrdering::SeqCst);
                    if prev <= 1 {
                        parent_to_promote = Some(p_id.clone());
                    }
                }
            }
        }

        if remove_now {
            self.jobs.remove(job_id);
        } else {
            q.completed_ids.insert(job_id.to_string(), now);
        }

        // Reschedule recurring job if needed
        if let Some(rpt) = repeat_to_schedule {
            let _ = self.push_job(rpt);
        }

        // Promote parent in flow if all children completed
        if let Some(p_id) = parent_to_promote {
            self.parent_pending.remove(&p_id);
            let parent_queue = self.jobs.get(&p_id).map(|j| j.queue_name.clone()).unwrap_or_else(|| queue_name.to_string());
            let _ = self.promote(&parent_queue, &p_id);
        }

        Ok(())
    }

    /// Fails job; if retries remain, schedules retry with exponential backoff; else marks failed.
    /// Returns true if permanently failed, false if scheduled for retry.
    pub fn fail_job(
        &self,
        queue_name: &str,
        job_id: &str,
        error_msg: &str,
    ) -> Result<bool, TurboPulseError> {
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();
        q.active_ids.remove(job_id);

        let mut remove_now = false;
        let mut is_final = false;

        if let Some(mut job) = self.jobs.get_mut(job_id) {
            let max_retries = job.config.max_retries.unwrap_or(3);
            if job.attempts_made < max_retries {
                // Compute backoff
                let backoff_type = job.config.backoff_type.unwrap_or(BackoffType::Exponential);
                let base_ms = job.config.backoff_base_ms.unwrap_or(1000) as u64;
                let max_ms = job.config.backoff_max_ms.unwrap_or(60000) as u64;
                let jitter = job.config.jitter.unwrap_or(true);

                let delay = calculate_backoff(job.attempts_made, backoff_type, base_ms, max_ms, jitter);

                job.state = JobState::Delayed;
                job.delay_until_ms = now + delay;
                job.failed_reason = Some(format!("Retry {}/{}: {}", job.attempts_made, max_retries, error_msg));

                let mut delayed_guard = q.delayed.lock();
                delayed_guard.push(DelayedJob {
                    delay_until_ms: job.delay_until_ms,
                    job_id: job_id.to_string(),
                });

                return Ok(false);
            } else {
                job.state = JobState::Failed;
                job.finished_at = Some(now);
                job.failed_reason = Some(error_msg.to_string());
                is_final = true;

                if job.config.remove_on_fail.unwrap_or(false) {
                    remove_now = true;
                }
            }
        }

        if is_final {
            if remove_now {
                self.jobs.remove(job_id);
            } else {
                q.failed_ids.insert(job_id.to_string(), now);
            }
        }

        Ok(is_final)
    }

    /// Promotes a delayed or waiting job to immediate waiting queue
    pub fn promote(&self, queue_name: &str, job_id: &str) -> bool {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            let actual_queue = if !job.queue_name.is_empty() {
                job.queue_name.clone()
            } else {
                queue_name.to_string()
            };
            let q = self.get_or_create_queue(&actual_queue);
            job.state = JobState::Waiting;
            job.delay_until_ms = 0;
            let seq = q.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
            let mut waiting_guard = q.waiting.lock();
            waiting_guard.push(PrioritizedJob {
                priority: job.config.priority.unwrap_or(0),
                sequence: seq,
                job_id: job_id.to_string(),
            });
            true
        } else {
            false
        }
    }

    /// Cleans jobs older than grace_ms in the specified state ('completed', 'failed', 'wait')
    pub fn clean(&self, queue_name: &str, grace_ms: u64, limit: u32, state_type: &str) -> u32 {
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();
        let mut cleaned = 0;

        match state_type {
            "completed" => {
                let to_remove: Vec<String> = q.completed_ids
                    .iter()
                    .filter(|entry| now.saturating_sub(*entry.value()) >= grace_ms)
                    .take(limit as usize)
                    .map(|entry| entry.key().clone())
                    .collect();

                for id in to_remove {
                    q.completed_ids.remove(&id);
                    self.jobs.remove(&id);
                    cleaned += 1;
                }
            }
            "failed" => {
                let to_remove: Vec<String> = q.failed_ids
                    .iter()
                    .filter(|entry| now.saturating_sub(*entry.value()) >= grace_ms)
                    .take(limit as usize)
                    .map(|entry| entry.key().clone())
                    .collect();

                for id in to_remove {
                    q.failed_ids.remove(&id);
                    self.jobs.remove(&id);
                    cleaned += 1;
                }
            }
            _ => {}
        }

        cleaned
    }

    /// Recovers stalled active jobs whose heartbeats expired
    pub fn recover_stalled(&self, queue_name: &str, lock_duration_ms: u64, max_stalled: u32) -> u32 {
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();
        let mut recovered = 0;

        let stalled_ids: Vec<String> = q.active_ids
            .iter()
            .filter(|entry| now.saturating_sub(*entry.value()) >= lock_duration_ms)
            .map(|entry| entry.key().clone())
            .collect();

        for id in stalled_ids {
            q.active_ids.remove(&id);
            if let Some(mut job) = self.jobs.get_mut(&id) {
                job.stalled_count += 1;
                if job.stalled_count <= max_stalled {
                    // Re-enqueue for retry
                    job.state = JobState::Waiting;
                    let seq = q.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
                    let mut waiting = q.waiting.lock();
                    waiting.push(PrioritizedJob {
                        priority: job.config.priority.unwrap_or(0),
                        sequence: seq,
                        job_id: id,
                    });
                    recovered += 1;
                } else {
                    // Mark as stalled permanently
                    job.state = JobState::Failed;
                    job.finished_at = Some(now);
                    job.failed_reason = Some(format!("Stalled {} times (exceeded maxStalledCount {})", job.stalled_count, max_stalled));
                    q.failed_ids.insert(id, now);
                }
            }
        }

        recovered
    }

    /// Updates arbitrary job binary payload
    pub fn update_data(&self, _queue_name: &str, job_id: &str, data: Vec<u8>) -> Result<(), TurboPulseError> {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.data = data;
            Ok(())
        } else {
            Err(TurboPulseError::JobNotFound(job_id.to_string()))
        }
    }

    pub fn update_progress(
        &self,
        _queue_name: &str,
        job_id: &str,
        progress: u32,
    ) -> Result<(), TurboPulseError> {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.progress = progress.min(100);
            Ok(())
        } else {
            Err(TurboPulseError::JobNotFound(job_id.to_string()))
        }
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobRecord> {
        self.jobs.get(job_id).map(|r| r.clone())
    }

    pub fn get_counts(&self, queue_name: &str) -> JobCounts {
        if let Some(q) = self.queues.get(queue_name) {
            JobCounts {
                waiting: q.waiting.lock().len() as u32,
                active: q.active_ids.len() as u32,
                delayed: q.delayed.lock().len() as u32,
                completed: q.completed_ids.len() as u32,
                failed: q.failed_ids.len() as u32,
            }
        } else {
            JobCounts {
                waiting: 0,
                active: 0,
                delayed: 0,
                completed: 0,
                failed: 0,
            }
        }
    }

    pub fn pause(&self, queue_name: &str) {
        let q = self.get_or_create_queue(queue_name);
        q.paused.store(true, AtomicOrdering::Relaxed);
    }

    pub fn resume(&self, queue_name: &str) {
        let q = self.get_or_create_queue(queue_name);
        q.paused.store(false, AtomicOrdering::Relaxed);
    }

    pub fn drain(&self, queue_name: &str) {
        if let Some(q) = self.queues.get(queue_name) {
            let mut waiting = q.waiting.lock();
            while let Some(item) = waiting.pop() {
                self.jobs.remove(&item.job_id);
            }
            let mut delayed = q.delayed.lock();
            while let Some(item) = delayed.pop() {
                self.jobs.remove(&item.job_id);
            }
        }
    }
}
