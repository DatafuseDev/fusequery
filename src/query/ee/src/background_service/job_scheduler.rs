// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tracing::info;
use common_base::base::{tokio};
use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::sync::mpsc::{Receiver, Sender, };
use common_exception::Result;
use common_meta_app::background::{BackgroundJobInfo, BackgroundJobState, BackgroundJobType};

use crate::background_service::job::BoxedJob;
use crate::background_service::job::Job;


pub struct JobScheduler {
    one_shot_jobs: Vec<BoxedJob>,
    scheduled_jobs: Vec<BoxedJob>,
    pub job_tick_interval: Duration,
    pub finish_tx: Arc<Mutex<Sender<u64>>>,
    pub finish_rx: Arc<Mutex<Receiver<u64>>>,
    pub suspend_tx: Arc<Mutex<Sender<()>>>,
    pub suspend_rx: Arc<Mutex<Receiver<()>>>,

}

impl JobScheduler {
    /// Creates a new runner based on the given SchedulerConfig
    pub fn new() -> Self {
        let (finish_tx, finish_rx) = tokio::sync::mpsc::channel(100);
        let (suspend_tx, suspend_rx) = tokio::sync::mpsc::channel(100);
        Self {
            one_shot_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            job_tick_interval: Duration::from_secs(5),
            finish_tx: Arc::new(Mutex::new(finish_tx)),
            finish_rx: Arc::new(Mutex::new(finish_rx)),

            suspend_tx: Arc::new(Mutex::new(suspend_tx)),
            suspend_rx: Arc::new(Mutex::new(suspend_rx)),
        }
    }

    pub fn add_job(&mut self, job: impl Job + Send + Sync + Clone + 'static) -> Result<()> {
        if job.get_info().job_params.is_none() {
            return Ok(())
        }
        match job.get_info().job_params.unwrap().job_type  {
            BackgroundJobType::ONESHOT => {
                self.one_shot_jobs.push(Box::new(job) as BoxedJob);
            }
            BackgroundJobType::CRON | BackgroundJobType::INTERVAL => {
                self.scheduled_jobs.push(Box::new(job) as BoxedJob);
            }
        }
        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        let one_shot_jobs = Arc::new(&self.one_shot_jobs);
        if !one_shot_jobs.is_empty() {
            info!(background = true, "start one_shot jobs");
            Self::check_and_run_jobs(one_shot_jobs.clone()).await;
            let mut finished_one_shot_jobs = vec![];
            while let Some(i) = self.finish_rx.clone().lock().await.recv().await {
                finished_one_shot_jobs.push(i);
                if finished_one_shot_jobs.len() == one_shot_jobs.len() {
                    break;
                }
            }
        }
        info!(background = true, "start scheduled jobs");
        Self::start_scheduled_jobs(Arc::new(&self.scheduled_jobs), std::time::Duration::from_secs(1)).await?;

        Ok(())
    }

    pub async fn start_scheduled_jobs(scheduled_jobs: Arc<&Vec<BoxedJob>>, tick_duration: std::time::Duration) -> Result<()> {
        if scheduled_jobs.is_empty() {
            return Ok(())
        }
        let mut job_interval = tokio::time::interval(tick_duration);
        loop {
            job_interval.tick().await;
            Self::check_and_run_jobs(Arc::new(&scheduled_jobs)).await;
        }
    }
    async fn check_and_run_jobs(jobs: Arc<&Vec<BoxedJob>>) {
        let job_futures = jobs
            .iter()
            .map(|job| {
                let j = job.box_clone();
                Self::check_and_run_job(j)
            })
            .collect::<Vec<_>>();
        for job in job_futures {
            job.await.unwrap();
        }

    }
    // Checks and runs a single [Job](crate::Job)
    async fn check_and_run_job(mut job: BoxedJob) -> Result<()> {
        if !Self::should_run_job(&job.get_info(), Utc::now()) {
            info!(background = true, "Job is not ready to run");
            return Ok(());
        }
        let info = job.get_info();
        let params = info.job_params.unwrap();
        let mut status = info.job_status.unwrap();
        status.next_task_scheduled_time = params.get_next_running_time(Utc::now());
        job.update_job_status(status.clone()).await?;
        tokio::spawn(async move {
            job.run().await
        });
        Ok(())
    }

    // returns true if the job should be run
    pub fn should_run_job(job_info: &BackgroundJobInfo, time: DateTime<Utc>) -> bool {
        if job_info.job_status.clone().is_none() || job_info.job_params.clone().is_none() {
            return false;
        }

        let job_params = &job_info.job_params.clone().unwrap();
        let job_status = &job_info.job_status.clone().unwrap();
        if job_status.job_state == BackgroundJobState::FAILED || job_status.job_state == BackgroundJobState::SUSPENDED {
            return false;
        }
        return match job_params.job_type {
            BackgroundJobType::INTERVAL | BackgroundJobType::CRON => {
                match job_status.next_task_scheduled_time {
                    None => {
                        true
                    }
                    Some(expected) => {
                        expected < time
                    }
                }
            }

            BackgroundJobType::ONESHOT => { true }
        }
    }
}
