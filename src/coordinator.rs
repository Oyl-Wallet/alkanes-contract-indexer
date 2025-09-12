use anyhow::Result;
use deezel_common::traits::MetashrewRpcProvider;
use tracing::{info, error};

use crate::{pipeline::{BlockContext, Pipeline}, progress::ProgressStore};

pub struct CatchUpCoordinator<P: MetashrewRpcProvider> {
    provider: P,
    pipeline: Pipeline,
    progress: ProgressStore,
    start_height: Option<u64>,
}

impl<P: MetashrewRpcProvider> CatchUpCoordinator<P> {
    pub fn new(provider: P, pipeline: Pipeline, progress: ProgressStore, start_height: Option<u64>) -> Self {
        Self { provider, pipeline, progress, start_height }
    }

    // Run a single pass: compute [next..=tip] to process sequentially and advance progress
    pub async fn run_once(&self) -> Result<()> {
        // Only run catch-up when a start height is provided.
        if self.start_height.is_none() {
            return Ok(());
        }

        let tip = self.provider.get_metashrew_height().await?;
        let last = self.progress.get_last_processed_height().await?;

        let next = match (last, self.start_height) {
            (Some(l), _) => l.saturating_add(1),
            (None, Some(s)) => s,
            (None, None) => return Ok(()),
        };

        if next > tip { return Ok(()); }

        for h in next..=tip {
            info!(height = h, "catch-up: processing block sequentially");
            if let Err(e) = self.pipeline.process_block_sequential(BlockContext { height: h }).await {
                error!(height = h, error = %e, "catch-up block processing failed");
                break;
            }
            self.progress.set_last_processed_height(h).await?;
        }
        Ok(())
    }
}


