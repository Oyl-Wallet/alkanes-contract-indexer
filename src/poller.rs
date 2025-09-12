use anyhow::Result;
use deezel_common::{provider::ConcreteProvider, traits::MetashrewRpcProvider};
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info, warn};

use crate::pipeline::{BlockContext, Pipeline};

pub struct BlockPoller {
    provider: ConcreteProvider,
    pipeline: Pipeline,
    poll_interval_ms: u64,
}

impl BlockPoller {
    pub fn new(provider: ConcreteProvider, pipeline: Pipeline, poll_interval_ms: u64) -> Self {
        Self { provider, pipeline, poll_interval_ms }
    }

    pub async fn run(self) {
        let mut last_height: Option<u64> = None;
        let mut backoff_ms: u64 = self.poll_interval_ms.max(250);
        loop {
            let tick_start = Instant::now();
            match self.provider.get_metashrew_height().await {
                Ok(height) => {
                    backoff_ms = self.poll_interval_ms; // reset on success
                    // Always fetch pools for the latest tip
                    if let Err(e) = self.pipeline.fetch_pools_for_tip(&self.provider, height).await {
                        error!(height, error = %e, "fetch_pools_for_tip failed");
                    }
                    match last_height {
                        None => {
                            info!(height, "initialized metashrew height");
                            last_height = Some(height);
                        }
                        Some(prev) if height > prev => {
                            for h in (prev + 1)..=height {
                                info!(height = h, "new block detected");
                                if let Err(e) = self.pipeline.process_block_sequential(BlockContext { height: h }).await {
                                    error!(height = h, error = %e, "block processing failed");
                                }
                                last_height = Some(h);
                            }
                        }
                        Some(prev) if height < prev => {
                            warn!(current = height, prev, "height decreased, possible reorg; updating pointer");
                            last_height = Some(height);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!(error = %e, "failed to fetch metashrew height");
                    // Exponential backoff with cap
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(30_000);
                }
            }

            let elapsed = tick_start.elapsed();
            let base = if backoff_ms == self.poll_interval_ms { self.poll_interval_ms } else { backoff_ms };
            let sleep_ms = base.saturating_sub(elapsed.as_millis() as u64);
            sleep(Duration::from_millis(sleep_ms.max(50))).await;
        }
    }
}


