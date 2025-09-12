use anyhow::{anyhow, Result};
use deezel_common::traits::MetashrewRpcProvider;

// Returns the canonical chain tip height by subtracting 1 from Metashrew's reported height,
// which is known to be off-by-one (reports next height).
pub async fn canonical_tip_height<P: MetashrewRpcProvider>(provider: &P) -> Result<u64> {
    let h = provider.get_metashrew_height().await?;
    if h == 0 {
        return Err(anyhow!("unexpected metashrew height 0"));
    }
    Ok(h - 1)
}


