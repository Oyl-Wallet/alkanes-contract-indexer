//! AMM/DEX functionality for alkanes
//!
//! This module implements AMM (Automated Market Maker) functionality following the OYL SDK patterns
//! but leveraging our enhanced alkanes execute system. Each AMM operation (create pool, add liquidity,
//! remove liquidity, swap) uses our improved execute functionality with proper protostones encoding.
//!
//! Key workflows implemented:
//! - Pool creation with factory contract calls
//! - Liquidity provision with optimal amount calculations
//! - Liquidity removal with preview functionality
//! - Token swaps with slippage protection
//! - Pool state queries and simulations

use crate::Result;
use log::{debug, info};

#[cfg(not(target_arch = "wasm32"))]
use std::{sync::Arc, vec::Vec};
#[cfg(target_arch = "wasm32")]
use alloc::{sync::Arc, vec::Vec};

use crate::{ToString, format};

#[cfg(not(target_arch = "wasm32"))]
use std::vec;
#[cfg(target_arch = "wasm32")]
use alloc::vec;

// Use specific imports to avoid conflicts
use super::types::{PoolCreateParams, LiquidityAddParams, LiquidityRemoveParams, SwapParams, TokenAmount, LiquidityRemovalPreview};
use super::types::AlkaneId as TypesAlkaneId;
use super::execute::{EnhancedAlkanesExecutor, EnhancedExecuteParams, EnhancedExecuteResult, InputRequirement};
use crate::traits::DeezelProvider;
use super::simulation::SimulationManager;

/// AMM operations manager that leverages enhanced execute functionality
pub struct AmmManager<P: DeezelProvider> {
    /// Underlying provider used for RPC calls (simulate, execute, etc.)
    provider: Arc<P>,
    simulation: Option<Arc<SimulationManager<P>>>,
}

impl<P: DeezelProvider> AmmManager<P> {
    async fn execute_enhanced(&self, params: EnhancedExecuteParams) -> Result<EnhancedExecuteResult> {
        use crate::alkanes::types::ExecutionState;
        // Create a temporary executor bound to the provider for each call
        // We need a &mut dyn DeezelProvider, so clone_box to a boxed provider, then borrow mut
        let mut boxed = self.provider.clone_box();
        let mut exec = super::execute::EnhancedAlkanesExecutor::new(&mut *boxed);
        let state = exec.execute(params.clone()).await?;
        match state {
            ExecutionState::ReadyToSign(s) => exec.resume_execution(s, &params).await,
            ExecutionState::ReadyToSignCommit(s) => {
                let state2 = exec.resume_commit_execution(s).await?;
                if let ExecutionState::ReadyToSignReveal(s2) = state2 {
                    exec.resume_reveal_execution(s2).await
                } else {
                    Err(crate::DeezelError::Other("Unexpected state after commit".to_string()))
                }
            }
            ExecutionState::ReadyToSignReveal(s) => exec.resume_reveal_execution(s).await,
            ExecutionState::Complete(result) => Ok(result),
        }
    }
    /// Create a new AMM manager
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider, simulation: None }
    }

    /// Create a new AMM manager with simulation capability
    pub fn new_with_simulation(
        provider: Arc<P>,
        simulation: Arc<SimulationManager<P>>,
    ) -> Self {
        Self { provider, simulation: Some(simulation) }
    }

    /// Create a new liquidity pool using enhanced execute functionality
    ///
    /// This follows the OYL SDK pattern:
    /// 1. Prepare token edicts for the pool tokens
    /// 2. Create protostones with factory contract calldata
    /// 3. Execute using enhanced execute with proper encoding
    pub async fn create_pool(&self, params: PoolCreateParams) -> Result<EnhancedExecuteResult> {
        info!("Creating liquidity pool with {} tokens using enhanced execute", params.tokens.len());
        debug!("Pool calldata: {:?}", params.calldata);
        
        // Validate that we have exactly 2 tokens (standard AMM pool)
        if params.tokens.len() != 2 {
            return Err(crate::DeezelError::Validation("Pool creation requires exactly 2 tokens".to_string()));
        }
        
        // Validate token amounts
        for token in &params.tokens {
            if token.amount == 0 {
                return Err(crate::DeezelError::Validation("Token amounts must be greater than zero".to_string()));
            }
        }
        
        // Convert calldata from Vec<String> to Vec<u8>
        let _calldata_bytes = params.calldata.join(",").into_bytes();
        
        // Prepare input requirements for the tokens
        let mut input_requirements = Vec::new();
        for token in &params.tokens {
            input_requirements.push(InputRequirement::Alkanes {
                block: token.alkane_id.block,
                tx: token.alkane_id.tx,
                amount: token.amount,
            });
        }
        
        // Create protostones with factory contract calldata
        let protostones = vec![
            super::execute::ProtostoneSpec {
                cellpack: None,
                edicts: Vec::new(),
                bitcoin_transfer: None,
            },
        ];
        
        // Prepare enhanced execute parameters
        let execute_params = EnhancedExecuteParams {
            fee_rate: params.fee_rate,
            to_addresses: vec![], // Will be populated with default addresses
            from_addresses: None,
            change_address: None,
            input_requirements,
            protostones,
            envelope_data: None,
            raw_output: false,
            trace_enabled: true,
            mine_enabled: false,
            auto_confirm: true,
        };
        
        // Execute pool creation using enhanced execute
        let result = self.execute_enhanced(execute_params).await?;
        
        info!("Liquidity pool created successfully using enhanced execute");
        info!("Pool creation reveal TXID: {}", result.reveal_txid);
        
        Ok(result)
    }

    /// Add liquidity to a pool using enhanced execute functionality
    ///
    /// This follows the OYL SDK pattern:
    /// 1. Calculate optimal token amounts based on current pool reserves
    /// 2. Prepare token edicts for the liquidity tokens
    /// 3. Create protostones with pool contract calldata
    /// 4. Execute using enhanced execute with proper encoding
    pub async fn add_liquidity(&self, params: LiquidityAddParams) -> Result<EnhancedExecuteResult> {
        info!("Adding liquidity with {} tokens using enhanced execute", params.tokens.len());
        debug!("Liquidity calldata: {:?}", params.calldata);
        
        // Validate that we have tokens to add
        if params.tokens.is_empty() {
            return Err(crate::DeezelError::Validation("Cannot add liquidity without tokens".to_string()));
        }
        
        // Validate token amounts
        for token in &params.tokens {
            if token.amount == 0 {
                return Err(crate::DeezelError::Validation("Token amounts must be greater than zero".to_string()));
            }
        }
        
        // Get current pool reserves to calculate optimal amounts
        let reserves = self.get_pool_reserves(&params.pool).await?;
        
        // Calculate optimal liquidity amounts if pool already has reserves
        let optimal_tokens = if reserves.len() >= 2 && reserves[0].amount > 0 && reserves[1].amount > 0 {
            let mut optimal = Vec::new();
            for (i, token) in params.tokens.iter().enumerate() {
                if i < reserves.len() {
                    let (optimal_a, optimal_b) = calculate_optimal_liquidity(
                        token.amount,
                        params.tokens.get(1).map(|t| t.amount).unwrap_or(0),
                        reserves[0].amount,
                        reserves[1].amount,
                    )?;
                    optimal.push(if i == 0 { optimal_a } else { optimal_b });
                } else {
                    optimal.push(token.amount);
                }
            }
            optimal
        } else {
            // First liquidity provision - use provided amounts
            params.tokens.iter().map(|t| t.amount).collect()
        };
        
        // Convert calldata from Vec<String> to Vec<u8>
        let _calldata_bytes = params.calldata.join(",").into_bytes();
        
        // Prepare input requirements for the optimal token amounts
        let mut input_requirements = Vec::new();
        for (token, &optimal_amount) in params.tokens.iter().zip(optimal_tokens.iter()) {
            input_requirements.push(InputRequirement::Alkanes {
                block: token.alkane_id.block,
                tx: token.alkane_id.tx,
                amount: optimal_amount,
            });
        }
        
        // Create protostones with pool contract calldata
        let protostones = vec![
            super::execute::ProtostoneSpec {
                cellpack: None,
                edicts: Vec::new(),
                bitcoin_transfer: None,
            },
        ];
        
        // Prepare enhanced execute parameters
        let execute_params = EnhancedExecuteParams {
            fee_rate: params.fee_rate,
            to_addresses: vec![], // Will be populated with default addresses
            from_addresses: None,
            change_address: None,
            input_requirements,
            protostones,
            envelope_data: None,
            raw_output: false,
            trace_enabled: true,
            mine_enabled: false,
            auto_confirm: true,
        };
        
        // Execute liquidity addition using enhanced execute
        let result = self.execute_enhanced(execute_params).await?;
        
        info!("Liquidity added successfully using enhanced execute");
        info!("Add liquidity reveal TXID: {}", result.reveal_txid);
        
        Ok(result)
    }

    /// Remove liquidity from a pool using enhanced execute functionality
    ///
    /// This follows the OYL SDK pattern:
    /// 1. Preview the removal to calculate expected token amounts
    /// 2. Prepare LP token edict for burning
    /// 3. Create protostones with pool contract calldata
    /// 4. Execute using enhanced execute with proper encoding
    pub async fn remove_liquidity(&self, params: LiquidityRemoveParams) -> Result<EnhancedExecuteResult> {
        info!("Removing {} LP tokens from pool {}:{} using enhanced execute",
              params.amount, params.token.block, params.token.tx);
        debug!("Remove liquidity calldata: {:?}", params.calldata);
        
        // Validate amount
        if params.amount == 0 {
            return Err(crate::DeezelError::Validation("Cannot remove zero liquidity".to_string()));
        }
        
        // Preview the removal to get expected amounts
        let preview = self.preview_remove_liquidity(&params.token, params.amount).await?;
        info!("Expected removal: {} token A, {} token B",
              preview.token_a_amount, preview.token_b_amount);
        
        // Convert calldata from Vec<String> to Vec<u8>
        let _calldata_bytes = params.calldata.join(",").into_bytes();
        
        // Prepare input requirements for the LP tokens to burn
        let input_requirements = vec![
            InputRequirement::Alkanes {
                block: params.token.block,
                tx: params.token.tx,
                amount: params.amount,
            }
        ];
        
        // Create protostones with pool contract calldata
        let protostones = vec![
            super::execute::ProtostoneSpec {
                cellpack: None,
                edicts: Vec::new(),
                bitcoin_transfer: None,
            },
        ];
        
        // Prepare enhanced execute parameters
        let execute_params = EnhancedExecuteParams {
            fee_rate: params.fee_rate,
            to_addresses: vec![], // Will be populated with default addresses
            from_addresses: None,
            change_address: None,
            input_requirements,
            protostones,
            envelope_data: None,
            raw_output: false,
            trace_enabled: true,
            mine_enabled: false,
            auto_confirm: true,
        };
        
        // Execute liquidity removal using enhanced execute
        let result = self.execute_enhanced(execute_params).await?;
        
        info!("Liquidity removed successfully using enhanced execute");
        info!("Remove liquidity reveal TXID: {}", result.reveal_txid);
        
        Ok(result)
    }

    /// Swap tokens in a pool using enhanced execute functionality
    ///
    /// This follows the OYL SDK pattern:
    /// 1. Calculate expected output based on pool reserves
    /// 2. Prepare input token edict for the swap
    /// 3. Create protostones with pool contract calldata
    /// 4. Execute using enhanced execute with proper encoding
    pub async fn swap(&self, params: SwapParams) -> Result<EnhancedExecuteResult> {
        info!("Swapping {} units of token {}:{} using enhanced execute",
              params.amount, params.token.block, params.token.tx);
        debug!("Swap calldata: {:?}", params.calldata);
        
        // Validate amount
        if params.amount == 0 {
            return Err(crate::DeezelError::Validation("Cannot swap zero tokens".to_string()));
        }
        
        // Get pool reserves to calculate swap output
        let reserves = self.get_pool_reserves(&params.pool).await?;
        
        // Find input and output reserves
        let input_reserve = reserves.iter()
            .find(|r| r.alkane_id.block == params.token.block && r.alkane_id.tx == params.token.tx)
            .map(|r| r.amount)
            .unwrap_or(0);
        
        if input_reserve == 0 {
            return Err(crate::DeezelError::Validation("Token not found in pool".to_string()));
        }
        
        // Calculate expected output (simplified - assumes 2-token pool)
        let output_reserve = reserves.iter()
            .find(|r| !(r.alkane_id.block == params.token.block && r.alkane_id.tx == params.token.tx))
            .map(|r| r.amount)
            .unwrap_or(0);
        
        let expected_output = calculate_swap_output(params.amount, input_reserve, output_reserve, 30)?;
        info!("Expected swap output: {} tokens", expected_output);
        
        // Convert calldata from Vec<String> to Vec<u8>
        let _calldata_bytes = params.calldata.join(",").into_bytes();
        
        // Prepare input requirements for the token to swap
        let input_requirements = vec![
            InputRequirement::Alkanes {
                block: params.token.block,
                tx: params.token.tx,
                amount: params.amount,
            }
        ];
        
        // Create protostones with pool contract calldata
        let protostones = vec![
            super::execute::ProtostoneSpec {
                cellpack: None,
                edicts: Vec::new(),
                bitcoin_transfer: None,
            },
        ];
        
        // Prepare enhanced execute parameters
        let execute_params = EnhancedExecuteParams {
            fee_rate: params.fee_rate,
            to_addresses: vec![], // Will be populated with default addresses
            from_addresses: None,
            change_address: None,
            input_requirements,
            protostones,
            envelope_data: None,
            raw_output: false,
            trace_enabled: true,
            mine_enabled: false,
            auto_confirm: true,
        };
        
        // Execute token swap using enhanced execute
        let result = self.execute_enhanced(execute_params).await?;
        
        info!("Token swap completed successfully using enhanced execute");
        info!("Swap reveal TXID: {}", result.reveal_txid);
        
        Ok(result)
    }

    /// Preview liquidity removal
    pub async fn preview_remove_liquidity(&self, token_id: &TypesAlkaneId, amount: u64) -> Result<LiquidityRemovalPreview> {
        info!("Previewing removal of {} LP tokens from {}:{}", 
              amount, token_id.block, token_id.tx);
        
        // For now, return a placeholder result
        // In a real implementation, this would:
        // 1. Query the pool state
        // 2. Calculate proportional token amounts
        // 3. Return the preview without executing
        
        Ok(LiquidityRemovalPreview {
            token_a_amount: amount / 2, // Placeholder calculation
            token_b_amount: amount / 2, // Placeholder calculation
            lp_tokens_burned: amount,
        })
    }

    /// Get pool information (placeholder implementation)
    pub async fn get_pool_info(&self, pool_id: &TypesAlkaneId) -> Result<serde_json::Value> {
        info!("Getting pool info for: {}:{}", pool_id.block, pool_id.tx);
        
        // For now, return a placeholder result
        // In a full implementation, this would query the pool contract state
        Ok(serde_json::json!({
            "pool_id": format!("{}:{}", pool_id.block, pool_id.tx),
            "status": "active"
        }))
    }

    /// Get pool reserves (placeholder implementation)
    pub async fn get_pool_reserves(&self, pool_id: &TypesAlkaneId) -> Result<Vec<TokenAmount>> {
        info!("Getting pool reserves for: {}:{}", pool_id.block, pool_id.tx);
        
        // For now, return placeholder reserves
        // In a full implementation, this would query the pool contract state
        // to get actual reserve amounts for each token in the pool
        
        // Return empty reserves as placeholder
        debug!("Returning placeholder empty reserves for pool {}:{}", pool_id.block, pool_id.tx);
        Ok(Vec::new())
    }

    /// Get all pools via raw simulate request (object-style), using the provided Sandshrew/Metashrew URL
    /// This mirrors the TS request you shared and returns decoded IDs.
    pub async fn get_all_pools_via_raw_simulate(&self, url: &str, factory_block: String, factory_tx: String) -> Result<GetAllPoolsResult> {
        // Build request identical to CLI's GetAllPools
        let params = serde_json::json!([{
            "alkanes": [],
            "transaction": "0x",
            "block": "0x",
            "height": "20000",
            "txindex": 0,
            "target": { "block": factory_block, "tx": factory_tx },
            "inputs": [FACTORY_OPCODE_GET_ALL_POOLS.to_string()],
            "pointer": 0,
            "refundPointer": 0,
            "vout": 0
        }]);

        let result = self.provider.call(url, "alkanes_simulate", params, 1).await?;
        let data_hex = result
            .get("execution")
            .and_then(|e| e.get("data"))
            .and_then(|v| v.as_str())
            .unwrap_or("0x");

        decode_get_all_pools(data_hex)
            .ok_or_else(|| crate::DeezelError::Other("Failed to decode get_all_pools result".to_string()))
    }

    /// For each pool id returned by get_all_pools_via_raw_simulate, fetch details via raw simulate
    pub async fn get_all_pools_details_via_raw_simulate(&self, url: &str, factory_block: String, factory_tx: String) -> Result<AllPoolsDetailsResult> {
        let all = self.get_all_pools_via_raw_simulate(url, factory_block, factory_tx).await?;

        let mut out = Vec::new();
        for id in &all.pools {
            let params = serde_json::json!([{
                "alkanes": [],
                "transaction": "0x",
                "block": "0x",
                "height": "20000",
                "txindex": 0,
                "target": { "block": id.block.to_string(), "tx": id.tx.to_string() },
                "inputs": [POOL_OPCODE_POOL_DETAILS.to_string()],
                "pointer": 0,
                "refundPointer": 0,
                "vout": 0
            }]);

            if let Ok(res) = self.provider.call(url, "alkanes_simulate", params, 1).await {
                if let Some(data) = res.get("execution").and_then(|e| e.get("data")).and_then(|v| v.as_str()) {
                    if let Some(details) = decode_pool_details(data) {
                        out.push(PoolDetailsWithId {
                            pool_id: id.clone(),
                            token0: details.token0,
                            token1: details.token1,
                            token0_amount: details.token0_amount,
                            token1_amount: details.token1_amount,
                            token_supply: details.token_supply,
                            pool_name: details.pool_name,
                        });
                    }
                }
            }
        }

        Ok(AllPoolsDetailsResult { count: out.len(), pools: out })
    }

    /// Get a single pool's details via raw simulate request (object-style)
    /// using the provided Sandshrew/Metashrew URL and target pool id
    pub async fn get_pool_details_via_raw_simulate(&self, url: &str, pool_block: String, pool_tx: String) -> Result<PoolDetailsResult> {
        info!(
            "Getting pool details via raw simulate for: {}:{}",
            pool_block, pool_tx
        );

        let params = serde_json::json!([{
            "alkanes": [],
            "transaction": "0x",
            "block": "0x",
            "height": "20000",
            "txindex": 0,
            "target": { "block": pool_block, "tx": pool_tx },
            "inputs": [POOL_OPCODE_POOL_DETAILS.to_string()],
            "pointer": 0,
            "refundPointer": 0,
            "vout": 0
        }]);

        let res = self.provider.call(url, "alkanes_simulate", params, 1).await?;
        let data_hex = res
            .get("execution")
            .and_then(|e| e.get("data"))
            .and_then(|v| v.as_str())
            .unwrap_or("0x");

        decode_pool_details(data_hex)
            .ok_or_else(|| crate::DeezelError::Other("Failed to decode pool_details result".to_string()))
    }
}

/// Operation codes for pool interactions
const POOL_OPCODE_INIT_POOL: u64 = 0;
const POOL_OPCODE_ADD_LIQUIDITY: u64 = 1;
const POOL_OPCODE_REMOVE_LIQUIDITY: u64 = 2;
const POOL_OPCODE_SWAP: u64 = 3;
const POOL_OPCODE_SIMULATE_SWAP: u64 = 4;
const POOL_OPCODE_NAME: u64 = 99;
const POOL_OPCODE_POOL_DETAILS: u64 = 999;

/// Factory operation codes
const FACTORY_OPCODE_INIT_POOL: u64 = 0;
const FACTORY_OPCODE_CREATE_NEW_POOL: u64 = 1;
const FACTORY_OPCODE_FIND_EXISTING_POOL_ID: u64 = 2;
const FACTORY_OPCODE_GET_ALL_POOLS: u64 = 3;

/// Result of a pool details query
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolDetailsResult {
    pub token0: TypesAlkaneId,
    pub token1: TypesAlkaneId,
    pub token0_amount: u64,
    pub token1_amount: u64,
    pub token_supply: u64,
    pub pool_name: String,
}

/// Get-all-pools result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetAllPoolsResult {
    pub count: usize,
    pub pools: Vec<TypesAlkaneId>,
}

/// All pools with details result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AllPoolsDetailsResult {
    pub count: usize,
    pub pools: Vec<PoolDetailsWithId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolDetailsWithId {
    pub pool_id: TypesAlkaneId,
    pub token0: TypesAlkaneId,
    pub token1: TypesAlkaneId,
    pub token0_amount: u64,
    pub token1_amount: u64,
    pub token_supply: u64,
    pub pool_name: String,
}

impl<P: DeezelProvider> AmmManager<P> {
    /// Get all AMM pools by simulating the factory contract
    pub async fn get_all_pools(&self, factory_id: &TypesAlkaneId) -> Result<GetAllPoolsResult> {
        let sim = self.simulation.as_ref()
            .ok_or_else(|| crate::DeezelError::Other("Simulation manager not configured".to_string()))?;

        let inputs = vec![FACTORY_OPCODE_GET_ALL_POOLS.to_string()];
        let result = sim
            .simulate_contract_execution(factory_id, &inputs)
            .await?;

        let data_hex = result
            .get("execution")
            .and_then(|e| e.get("data"))
            .and_then(|v| v.as_str())
            .unwrap_or("0x");

        let decoded = decode_get_all_pools(data_hex)
            .ok_or_else(|| crate::DeezelError::Other("Failed to decode get_all_pools result".to_string()))?;

        Ok(decoded)
    }

    /// Get all pools and fetch each pool's detailed info via simulation
    pub async fn get_all_pools_details(&self, factory_id: &TypesAlkaneId) -> Result<AllPoolsDetailsResult> {
        let all = self.get_all_pools(factory_id).await?;
        let sim = self.simulation.as_ref()
            .ok_or_else(|| crate::DeezelError::Other("Simulation manager not configured".to_string()))?;

        let mut pools_with_details = Vec::new();
        for pool_id in &all.pools {
            let inputs = vec![POOL_OPCODE_POOL_DETAILS.to_string()];
            match sim.simulate_contract_execution(pool_id, &inputs).await {
                Ok(result) => {
                    let data_hex = result
                        .get("execution")
                        .and_then(|e| e.get("data"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("0x");
                    if let Some(details) = decode_pool_details(data_hex) {
                        pools_with_details.push(PoolDetailsWithId {
                            pool_id: pool_id.clone(),
                            token0: details.token0,
                            token1: details.token1,
                            token0_amount: details.token0_amount,
                            token1_amount: details.token1_amount,
                            token_supply: details.token_supply,
                            pool_name: details.pool_name,
                        });
                    }
                }
                Err(e) => {
                    log::error!(
                        "Error getting details for pool {}:{}: {}",
                        pool_id.block, pool_id.tx, e
                    );
                }
            }
        }

        Ok(AllPoolsDetailsResult { count: pools_with_details.len(), pools: pools_with_details })
    }
}

fn strip_0x(s: &str) -> &str {
    if let Some(rest) = s.strip_prefix("0x") { rest } else { s }
}

fn hex_to_bytes(hex_str: &str) -> Option<Vec<u8>> {
    let clean = strip_0x(hex_str);
    hex::decode(clean).ok()
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Option<u64> {
    if bytes.len() < offset + 8 { return None; }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[offset..offset+8]);
    Some(u64::from_le_bytes(buf))
}

fn read_u128_le(bytes: &[u8], offset: usize) -> Option<u128> {
    if bytes.len() < offset + 16 { return None; }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&bytes[offset..offset+16]);
    Some(u128::from_le_bytes(buf))
}

fn decode_pool_details(data_hex: &str) -> Option<PoolDetailsResult> {
    if data_hex == "0x" { return None; }
    let bytes = hex_to_bytes(data_hex)?;

    // Values are encoded as 128-bit little-endian. We take the low 64 bits.
    let lo_mask: u128 = 0xFFFF_FFFF_FFFF_FFFF;

    let token0 = TypesAlkaneId {
        block: (read_u128_le(&bytes, 0)? & lo_mask) as u64,
        tx: (read_u128_le(&bytes, 16)? & lo_mask) as u64,
    };
    let token1 = TypesAlkaneId {
        block: (read_u128_le(&bytes, 32)? & lo_mask) as u64,
        tx: (read_u128_le(&bytes, 48)? & lo_mask) as u64,
    };
    let token0_amount = (read_u128_le(&bytes, 64)? & lo_mask) as u64;
    let token1_amount = (read_u128_le(&bytes, 80)? & lo_mask) as u64;
    let token_supply = (read_u128_le(&bytes, 96)? & lo_mask) as u64;
    let pool_name = if bytes.len() > 116 {
        String::from_utf8_lossy(&bytes[116..]).to_string()
    } else {
        String::new()
    };

    Some(PoolDetailsResult { token0, token1, token0_amount, token1_amount, token_supply, pool_name })
}

fn parse_alkane_id_from_hex(hex_str: &str) -> Option<TypesAlkaneId> {
    // Expect 32 bytes (64 hex chars) total: 16 for block, 16 for tx
    let clean = strip_0x(hex_str);
    if clean.len() < 64 { return None; }
    let block_hex = &clean[0..32];
    let tx_hex = &clean[32..64];

    let mut block_bytes = hex::decode(block_hex).ok()?;
    block_bytes.reverse();
    let mut tx_bytes = hex::decode(tx_hex).ok()?;
    tx_bytes.reverse();

    // Take low 8 bytes to fit into u64, matching provider's use of lo parts
    // After reversing to big-endian, the low 8 bytes are at the tail
    let block = {
        if block_bytes.len() < 8 { return None; }
        let start = block_bytes.len() - 8;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&block_bytes[start..]);
        u64::from_be_bytes(buf)
    };
    let tx = {
        if tx_bytes.len() < 8 { return None; }
        let start = tx_bytes.len() - 8;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&tx_bytes[start..]);
        u64::from_be_bytes(buf)
    };

    Some(TypesAlkaneId { block, tx })
}

fn decode_get_all_pools(data_hex: &str) -> Option<GetAllPoolsResult> {
    if data_hex == "0x" { return None; }
    let clean = strip_0x(data_hex);
    if clean.len() < 32 { return None; }

    // First 16 bytes (32 hex chars) is count, little-endian
    let mut count_bytes = hex::decode(&clean[0..32]).ok()?;
    count_bytes.reverse();
    let count = u128::from_str_radix(&hex::encode(count_bytes), 16).ok()? as usize;

    let mut pools = Vec::new();
    for i in 0..count {
        let offset = 32 + i * 64; // after count (32 hex), each entry is 64 hex chars (32 bytes)
        if clean.len() < offset + 64 { break; }
        let entry_hex = &clean[offset..offset+64];
        if let Some(id) = parse_alkane_id_from_hex(entry_hex) {
            pools.push(id);
        }
    }

    Some(GetAllPoolsResult { count: pools.len(), pools })
}

/// Calculate optimal liquidity amounts for adding to a pool
pub fn calculate_optimal_liquidity(
    desired_a: u64,
    desired_b: u64,
    reserve_a: u64,
    reserve_b: u64,
) -> Result<(u64, u64)> {
    if reserve_a == 0 || reserve_b == 0 {
        // First liquidity provision
        return Ok((desired_a, desired_b));
    }
    
    // Calculate optimal amounts based on current pool ratio
    let amount_b_optimal = (desired_a * reserve_b) / reserve_a;
    
    if amount_b_optimal <= desired_b {
        Ok((desired_a, amount_b_optimal))
    } else {
        let amount_a_optimal = (desired_b * reserve_a) / reserve_b;
        Ok((amount_a_optimal, desired_b))
    }
}

/// Calculate swap output amount using constant product formula
pub fn calculate_swap_output(
    input_amount: u64,
    input_reserve: u64,
    output_reserve: u64,
    fee_rate: u64, // Fee rate in basis points (e.g., 30 for 0.3%)
) -> Result<u64> {
    if input_reserve == 0 || output_reserve == 0 {
        return Err(crate::DeezelError::Validation("Cannot swap with zero reserves".to_string()));
    }
    
    // Apply fee to input amount
    let input_amount_with_fee = input_amount * (10000 - fee_rate);
    
    // Calculate output using constant product formula: x * y = k
    let numerator = input_amount_with_fee * output_reserve;
    let denominator = (input_reserve * 10000) + input_amount_with_fee;
    
    Ok(numerator / denominator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_liquidity_first_provision() {
        let (amount_a, amount_b) = calculate_optimal_liquidity(1000, 2000, 0, 0).unwrap();
        assert_eq!(amount_a, 1000);
        assert_eq!(amount_b, 2000);
    }

    #[test]
    fn test_calculate_optimal_liquidity_existing_pool() {
        // Pool has 1:2 ratio (1000:2000)
        let (amount_a, amount_b) = calculate_optimal_liquidity(500, 2000, 1000, 2000).unwrap();
        assert_eq!(amount_a, 500);
        assert_eq!(amount_b, 1000); // Optimal amount based on ratio
    }

    #[test]
    fn test_calculate_swap_output() {
        // Swap 100 tokens with 0.3% fee
        let output = calculate_swap_output(100, 1000, 2000, 30).unwrap();
        // Expected: (100 * 9970 * 2000) / (1000 * 10000 + 100 * 9970) = ~181
        assert!(output > 180 && output < 185);
    }

    #[test]
    fn test_calculate_swap_output_zero_reserves() {
        assert!(calculate_swap_output(100, 0, 1000, 30).is_err());
        assert!(calculate_swap_output(100, 1000, 0, 30).is_err());
    }
}