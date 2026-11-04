use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::config::BackoffType;
use crate::error::TurboPulseError;
use crate::queue::job::{JobCounts, JobRecord, JobState};
use crate::utils::time::{calculate_backoff, current_time_ms};

/// Element in the waiting max-heap
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
/// Provides full queue feature parity (priority, delays, retries with backoff, progress)
/// completely outside the V8 heap in native memory.
pub struct InMemoryQueue {
    queues: DashMap<String, Arc<QueueState>>,
    jobs: DashMap<String, JobRecord>,
    parent_dependencies: DashMap<String, Arc<Mutex<HashSet<String>>>>,
}

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
            jobs: DashMap::new(),
            parent_dependencies: DashMap::new(),
        }
    }

    pub fn register_parent_children(&self, parent_id: &str, child_ids: &[String]) {
        let entry = self
            .parent_dependencies
            .entry(parent_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(HashSet::new())))
            .clone();
        let mut set = entry.lock();
        for id in child_ids {
            set.insert(id.clone());
        }
    }

    pub fn resolve_parent_dep(
        &self,
        parent_queue: &str,
        parent_id: &str,
        child_id: &str,
    ) -> Result<bool, TurboPulseError> {
        let mut should_promote = false;
        if let Some(entry) = self.parent_dependencies.get(parent_id) {
            let mut set = entry.value().lock();
            set.remove(child_id);
            if set.is_empty() {
                should_promote = true;
            }
        }

        if should_promote {
            self.parent_dependencies.remove(parent_id);
            if let Some(mut parent) = self.jobs.get_mut(parent_id) {
                if parent.state == JobState::WaitingChildren {
                    parent.state = JobState::Waiting;
                    let pq = self.get_or_create_queue(parent_queue);
                    let seq = pq.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
                    let mut waiting_guard = pq.waiting.lock();
                    waiting_guard.push(PrioritizedJob {
                        priority: parent.config.priority.unwrap_or(0),
                        sequence: seq,
                        job_id: parent_id.to_string(),
                    });
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn get_or_create_queue(&self, queue_name: &str) -> Arc<QueueState> {
        self.queues
            .entry(queue_name.to_string())
            .or_insert_with(|| Arc::new(QueueState::new()))
            .clone()
    }

    pub fn push_job(&self, job: JobRecord) -> Result<(), TurboPulseError> {
        if self.jobs.contains_key(&job.id) {
            return Err(TurboPulseError::InvalidJobState(format!(
                "Job with ID {} already exists",
                job.id
            )));
        }

        let q = self.get_or_create_queue(&job.queue_name);
        let job_id = job.id.clone();
        let now = current_time_ms();

        if job.state == JobState::WaitingChildren {
            // Blocked: parent waits in WaitingChildren state until dependencies resolve
        } else if job.delay_until_ms > now {
            let mut delayed_guard = q.delayed.lock();
            delayed_guard.push(DelayedJob {
                delay_until_ms: job.delay_until_ms,
                job_id: job_id.clone(),
            });
        } else {
            let raw_seq = q.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
            let seq = if job.config.lifo.unwrap_or(false) {
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
        // Pre-validate: reject duplicates within batch or existing records
        let mut seen = HashSet::new();
        for job in &jobs {
            if !seen.insert(&job.id) || self.jobs.contains_key(&job.id) {
                return Err(TurboPulseError::InvalidJobState(format!(
                    "Bulk insert failed: duplicate job ID {} detected",
                    job.id
                )));
            }
        }

        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            let id = job.id.clone();
            self.push_job(job)?;
            ids.push(id);
        }
        Ok(ids)
    }

    pub fn fetch_next_batch(
        &self,
        queue_name: &str,
        count: usize,
        lock_duration_ms: Option<u64>,
    ) -> Result<Vec<JobRecord>, TurboPulseError> {
        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(j) = self.fetch_next_job(queue_name, lock_duration_ms)? {
                list.push(j);
            } else {
                break;
            }
        }
        Ok(list)
    }

    /// Moves any matured delayed jobs into the waiting queue and pops the next waiting job
    pub fn fetch_next_job(
        &self,
        queue_name: &str,
        lock_duration_ms: Option<u64>,
    ) -> Result<Option<JobRecord>, TurboPulseError> {
        let q = self.get_or_create_queue(queue_name);

        if q.paused.load(AtomicOrdering::Relaxed) {
            return Ok(None);
        }

        let now = current_time_ms();
        let lock_duration = lock_duration_ms.unwrap_or(30000);

        // 1. Promote matured delayed jobs into waiting
        {
            let mut delayed_guard = q.delayed.lock();
            let mut waiting_guard = q.waiting.lock();

            while let Some(top) = delayed_guard.peek() {
                if top.delay_until_ms <= now {
                    let delayed_item = delayed_guard.pop().unwrap();
                    if let Some(mut job) = self.jobs.get_mut(&delayed_item.job_id) {
                        job.state = JobState::Waiting;
                        let seq = q.sequence_counter.fetch_add(1, AtomicOrdering::Relaxed);
                        waiting_guard.push(PrioritizedJob {
                            priority: job.config.priority.unwrap_or(0),
                            sequence: seq,
                            job_id: delayed_item.job_id,
                        });
                    }
                } else {
                    break;
                }
            }
        }

        // 2. Pop highest priority waiting job
        let mut waiting_guard = q.waiting.lock();
        while let Some(p_job) = waiting_guard.pop() {
            if let Some(mut job) = self.jobs.get_mut(&p_job.job_id) {
                if job.state == JobState::Waiting {
                    job.state = JobState::Active;
                    job.attempts_made += 1;
                    job.processed_at = Some(now);
                    let lock_token = format!("{}-{}", now, uuid::Uuid::new_v4());
                    job.lock_token = Some(lock_token);

                    // Active sorted set equivalent: stores lease expiration
                    q.active_ids.insert(p_job.job_id.clone(), now + lock_duration);
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
        lock_token: &str,
    ) -> Result<bool, TurboPulseError> {
        if lock_token.trim().is_empty() {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Missing lock token".to_string(),
            ));
        }

        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();

        let mut job = match self.jobs.get_mut(job_id) {
            Some(j) => j,
            None => return Err(TurboPulseError::InvalidJobState("Job not found".to_string())),
        };

        if job.state != JobState::Active {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Job is not in Active state".to_string(),
            ));
        }

        if job.lock_token.as_deref() != Some(lock_token) {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Lock token mismatch".to_string(),
            ));
        }

        if let Some(expire) = q.active_ids.get(job_id) {
            if *expire < now {
                return Err(TurboPulseError::InvalidJobState(
                    "LOCK_LOST: Active lease has expired".to_string(),
                ));
            }
        } else {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Job is not in active index".to_string(),
            ));
        }

        // Invalidate lock token and remove from active index
        job.lock_token = None;
        q.active_ids.remove(job_id);

        job.state = JobState::Completed;
        job.finished_at = Some(now);
        job.return_value = return_value;
        job.progress = 100;

        let remove_now = job.config.remove_on_complete.unwrap_or(false);

        // Check repeat options
        let mut repeat_to_schedule: Option<JobRecord> = None;
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

        let parent_info = if let Some(ref p_id) = job.config.parent_id {
            Some((job.config.parent_queue.clone().unwrap_or_else(|| queue_name.to_string()), p_id.clone()))
        } else {
            None
        };

        drop(job);

        if remove_now {
            self.jobs.remove(job_id);
        } else {
            q.completed_ids.insert(job_id.to_string(), now);
        }

        if let Some(rpt) = repeat_to_schedule {
            let _ = self.push_job(rpt);
        }

        if let Some((pq, pid)) = parent_info {
            let _ = self.resolve_parent_dep(&pq, &pid, job_id);
        }

        Ok(true)
    }

    pub fn fail_job(
        &self,
        queue_name: &str,
        job_id: &str,
        error_msg: &str,
        lock_token: &str,
    ) -> Result<bool, TurboPulseError> {
        if lock_token.trim().is_empty() {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Missing lock token".to_string(),
            ));
        }

        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();

        let mut job = match self.jobs.get_mut(job_id) {
            Some(j) => j,
            None => return Err(TurboPulseError::InvalidJobState("Job not found".to_string())),
        };

        if job.state != JobState::Active {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Job is not in Active state".to_string(),
            ));
        }

        if job.lock_token.as_deref() != Some(lock_token) {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Lock token mismatch".to_string(),
            ));
        }

        if let Some(expire) = q.active_ids.get(job_id) {
            if *expire < now {
                return Err(TurboPulseError::InvalidJobState(
                    "LOCK_LOST: Active lease has expired".to_string(),
                ));
            }
        } else {
            return Err(TurboPulseError::InvalidJobState(
                "LOCK_LOST: Job is not in active index".to_string(),
            ));
        }

        job.lock_token = None;
        q.active_ids.remove(job_id);

        let max_retries = job.config.max_retries.unwrap_or(3);
        if job.attempts_made < max_retries {
            let backoff_type = job.config.backoff_type.unwrap_or(BackoffType::Exponential);
            let base_ms = job.config.backoff_base_ms.unwrap_or(1000) as u64;
            let max_ms = job.config.backoff_max_ms.unwrap_or(60000) as u64;
            let jitter = job.config.jitter.unwrap_or(true);

            let delay = calculate_backoff(job.attempts_made, backoff_type, base_ms, max_ms, jitter);

            job.state = JobState::Delayed;
            job.delay_until_ms = now + delay;
            job.failed_reason = Some(format!(
                "Retry {}/{}: {}",
                job.attempts_made, max_retries, error_msg
            ));

            let mut delayed_guard = q.delayed.lock();
            delayed_guard.push(DelayedJob {
                delay_until_ms: job.delay_until_ms,
                job_id: job_id.to_string(),
            });

            Ok(false)
        } else {
            job.state = JobState::Failed;
            job.finished_at = Some(now);
            job.failed_reason = Some(error_msg.to_string());

            let remove_now = job.config.remove_on_fail.unwrap_or(false);
            drop(job);

            if remove_now {
                self.jobs.remove(job_id);
            } else {
                q.failed_ids.insert(job_id.to_string(), now);
            }

            Ok(true)
        }
    }

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

    pub fn renew_lock(
        &self,
        queue_name: &str,
        job_id: &str,
        lock_token: &str,
        lock_duration_ms: u32,
    ) -> Result<bool, TurboPulseError> {
        if lock_token.trim().is_empty() {
            return Ok(false);
        }
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();

        if let Some(job) = self.jobs.get(job_id) {
            if job.state != JobState::Active {
                return Ok(false);
            }
            if job.lock_token.as_deref() != Some(lock_token) {
                return Ok(false);
            }
            if let Some(mut expire) = q.active_ids.get_mut(job_id) {
                if *expire < now {
                    return Ok(false);
                }
                *expire = now + lock_duration_ms as u64;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn clean(&self, queue_name: &str, grace_ms: u64, limit: u32, state_type: &str) -> u32 {
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();
        let mut cleaned = 0;

        match state_type {
            "completed" => {
                let to_remove: Vec<String> = q
                    .completed_ids
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
                let to_remove: Vec<String> = q
                    .failed_ids
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

    pub fn recover_stalled(
        &self,
        queue_name: &str,
        lock_duration_ms: u64,
        max_stalled: u32,
    ) -> u32 {
        let q = self.get_or_create_queue(queue_name);
        let now = current_time_ms();
        let mut recovered = 0;

        let stalled_ids: Vec<String> = q
            .active_ids
            .iter()
            .filter(|entry| {
                let expire = *entry.value();
                expire <= now || (lock_duration_ms > 0 && lock_duration_ms < 30000 && expire.saturating_sub(30000) + lock_duration_ms <= now)
            })
            .map(|entry| entry.key().clone())
            .collect();

        for id in stalled_ids {
            q.active_ids.remove(&id);
            if let Some(mut job) = self.jobs.get_mut(&id) {
                if job.state == JobState::Active {
                    job.stalled_count += 1;
                    job.lock_token = None;
                    if job.stalled_count <= max_stalled {
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
                        job.state = JobState::Failed;
                        job.finished_at = Some(now);
                        job.failed_reason = Some(format!(
                            "Stalled {} times (exceeded maxStalledCount {})",
                            job.stalled_count, max_stalled
                        ));
                        q.failed_ids.insert(id, now);
                    }
                }
            }
        }

        recovered
    }

    pub fn update_data(&self, _queue_name: &str, job_id: &str, data: Vec<u8>) -> Result<(), TurboPulseError> {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.data = data;
            Ok(())
        } else {
            Err(TurboPulseError::JobNotFound(job_id.to_string()))
        }
    }

    pub fn update_progress(&self, _queue_name: &str, job_id: &str, progress: u32) -> Result<(), TurboPulseError> {
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

    pub fn obliterate(&self, queue_name: &str, force: bool) -> Result<u32, TurboPulseError> {
        let q = self.get_or_create_queue(queue_name);
        if !force && !q.active_ids.is_empty() {
            return Err(TurboPulseError::InvalidJobState(
                "Cannot obliterate queue with active jobs without force=true".to_string(),
            ));
        }

        let mut deleted = 0;
        {
            let mut waiting = q.waiting.lock();
            while let Some(item) = waiting.pop() {
                self.jobs.remove(&item.job_id);
                deleted += 1;
            }
            let mut delayed = q.delayed.lock();
            while let Some(item) = delayed.pop() {
                self.jobs.remove(&item.job_id);
                deleted += 1;
            }
        }

        for item in q.active_ids.iter() {
            self.jobs.remove(item.key());
            deleted += 1;
        }
        q.active_ids.clear();

        for item in q.completed_ids.iter() {
            self.jobs.remove(item.key());
            deleted += 1;
        }
        q.completed_ids.clear();

        for item in q.failed_ids.iter() {
            self.jobs.remove(item.key());
            deleted += 1;
        }
        q.failed_ids.clear();

        Ok(deleted)
    }
}
