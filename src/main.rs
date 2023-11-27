use std::path::PathBuf;

use ethers::prelude::*;
use ethers::types::Call;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct BoostRelayDataEntry {
    slot: u64,
    proposer_fee_recipient: Address,
    #[serde(deserialize_with = "deserialize_u256_from_decimal")]
    value: U256,
    block_number: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OutputFileEntry {
    slot: u64,
    block_number: u64,
    #[serde(
        serialize_with = "serialize_u256_to_decimal",
        deserialize_with = "deserialize_u256_from_decimal"
    )]
    bid_value: U256,
    #[serde(
        serialize_with = "serialize_u256_to_decimal",
        deserialize_with = "deserialize_u256_from_decimal"
    )]
    balance_diff: U256,
    payment_type: String,
    withdrawals: usize,
    transfers: usize,
    transfers_in: usize,
    transfers_out: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferData {
    block_number: u64,
    tx_hash: H256,
    from: Address,
    to: Address,
    value: U256,
}

fn deserialize_u256_from_decimal<'de, D>(deserializer: D) -> Result<U256, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    U256::from_dec_str(&s).map_err(serde::de::Error::custom)
}

fn serialize_u256_to_decimal<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn extract_transfers(traces: &[Trace]) -> Vec<TransferData> {
    let mut transfers = Vec::new();
    for trace in traces {
        if let Trace {
            action:
                Action::Call(Call {
                    from,
                    to,
                    value,
                    call_type: CallType::Call,
                    ..
                }),
            error: None,
            block_number,
            transaction_hash: Some(tx_hash),
            ..
        } = trace
        {
            if value.is_zero() {
                continue;
            }
            transfers.push(TransferData {
                block_number: *block_number,
                tx_hash: *tx_hash,
                from: *from,
                to: *to,
                value: *value,
            });
        }
    }
    transfers
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProposerPayment {
    LastTxDirect {
        from: Address,
        to: Address,
        value: U256,
    },
    LastTxContract {
        from: Address,
        contract: Address,
        value: U256,
    },
    Coinbase(Address),
    Unknown,
}

impl ProposerPayment {
    fn is_last_tx(&self) -> bool {
        matches!(
            self,
            ProposerPayment::LastTxDirect { .. } | ProposerPayment::LastTxContract { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockProposerPaymentData {
    block_number: u64,
    fee_recipient: Address,
    bid_value: U256,
    fee_recipient_transfers: Vec<TransferData>,
    fee_recipient_withdrawals: Vec<Withdrawal>,
    payment: ProposerPayment,
    balance_diff: U256,
}

async fn get_block_proposer_payment_data(
    provider: &Provider<Http>,
    block_numer: u64,
    fee_recipient: Address,
    bid_value: U256,
) -> eyre::Result<BlockProposerPaymentData> {
    let transfers = {
        let trace = provider
            .trace_block(BlockNumber::Number(block_numer.into()))
            .await?;
        let mut transfers = extract_transfers(&trace);
        transfers.retain(|t| t.to == fee_recipient || t.from == fee_recipient);
        transfers
    };

    let (withdrawals, payment) = {
        let block = provider
            .get_block_with_txs(block_numer)
            .await?
            .ok_or_else(|| eyre::eyre!("block not found"))?;
        let withdrawals = {
            let mut withdrawals = block.withdrawals.unwrap_or_default();
            withdrawals.retain(|w| w.address == fee_recipient);
            withdrawals
        };

        let coinbase = block.author.unwrap_or_default();
        let payment = if coinbase == fee_recipient {
            ProposerPayment::Coinbase(coinbase)
        } else {
            if let Some(last_tx) = block.transactions.last() {
                if last_tx.to == Some(fee_recipient) {
                    ProposerPayment::LastTxDirect {
                        from: last_tx.from,
                        to: last_tx.to.unwrap(),
                        value: last_tx.value,
                    }
                } else {
                    if let Some(last_transfer) = transfers.last().cloned() {
                        if last_transfer.tx_hash == last_tx.hash
                            && last_transfer.to == fee_recipient
                        {
                            ProposerPayment::LastTxContract {
                                from: last_tx.from,
                                contract: last_tx.to.unwrap_or_default(),
                                value: last_transfer.value,
                            }
                        } else {
                            ProposerPayment::Unknown
                        }
                    } else {
                        ProposerPayment::Unknown
                    }
                }
            } else {
                ProposerPayment::Unknown
            }
        };
        (withdrawals, payment)
    };

    let balance_diff = {
        let balance_before = provider
            .get_balance(fee_recipient, Some((block_numer - 1u64).into()))
            .await?;
        let balance_after = provider
            .get_balance(fee_recipient, Some(block_numer.into()))
            .await?;

        balance_after
            .checked_sub(balance_before)
            .unwrap_or_default()
    };

    Ok(BlockProposerPaymentData {
        block_number: block_numer,
        fee_recipient,
        bid_value,
        fee_recipient_transfers: transfers,
        fee_recipient_withdrawals: withdrawals,
        payment,
        balance_diff,
    })
}

#[derive(Debug, clap::Parser)]
enum Command {
    #[clap(name = "file")]
    File {
        #[clap(long)]
        input: PathBuf,
        #[clap(long)]
        output: PathBuf,
    },
    #[clap(name = "block")]
    Block {
        #[clap(long)]
        number: u64,
        #[clap(long)]
        fee_recipient: Address,
        #[clap(long)]
        bid_value: String,
    },
}

#[derive(Debug, clap::Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
    #[clap(long, env = "ETH_RPC_URL")]
    eth_rpc_url: String,
    #[clap(long, env = "ETH_RPC_PAR", default_value = "10")]
    rpc_parallel: usize,
}

async fn process_input_entry(
    provider: &Provider<Http>,
    input: BoostRelayDataEntry,
) -> eyre::Result<OutputFileEntry> {
    let data = get_block_proposer_payment_data(
        &provider,
        input.block_number,
        input.proposer_fee_recipient,
        input.value,
    )
    .await?;
    Ok(OutputFileEntry {
        slot: input.slot,
        block_number: data.block_number,
        bid_value: data.bid_value,
        balance_diff: data.balance_diff,
        payment_type: match data.payment {
            ProposerPayment::LastTxDirect { .. } => "last_tx_direct".to_string(),
            ProposerPayment::LastTxContract { .. } => "last_tx_contract".to_string(),
            ProposerPayment::Coinbase(..) => "coinbase".to_string(),
            ProposerPayment::Unknown => "unknown".to_string(),
        },
        withdrawals: data.fee_recipient_withdrawals.len(),
        transfers: if data.payment.is_last_tx() {
            data.fee_recipient_transfers.len() - 1
        } else {
            data.fee_recipient_transfers.len()
        },
        transfers_in: data
            .fee_recipient_transfers
            .iter()
            .filter(|t| t.to == data.fee_recipient)
            .count()
            - if data.payment.is_last_tx() { 1 } else { 0 },
        transfers_out: data
            .fee_recipient_transfers
            .iter()
            .filter(|t| t.from == data.fee_recipient)
            .count(),
    })
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let provider = Provider::try_from(cli.eth_rpc_url.as_str())?;

    match cli.command {
        Command::Block {
            number,
            fee_recipient,
            bid_value,
        } => {
            let bid_value = U256::from_dec_str(&bid_value)?;
            let data = get_block_proposer_payment_data(&provider, number, fee_recipient, bid_value)
                .await?;
            println!("{:#?}", data);
        }
        Command::File { input, output } => {
            let processed_entries = if output.exists() {
                // read output file
                let mut reader = csv::Reader::from_path(&output)?;
                let mut entries = Vec::new();
                for entry in reader.deserialize() {
                    let entry: OutputFileEntry = entry?;
                    entries.push(entry);
                }
                entries
            } else {
                Vec::new()
            };

            let processed_set = processed_entries
                .iter()
                .map(|e| e.slot)
                .collect::<std::collections::HashSet<_>>();

            let input = {
                let input =
                    csv::Reader::from_path(&input)?.into_deserialize::<BoostRelayDataEntry>();
                let mut entries = Vec::new();
                for entry in input {
                    let entry = entry?;
                    if processed_set.contains(&entry.slot) {
                        continue;
                    }
                    entries.push(entry);
                }
                entries
            };

            let mut output = csv::Writer::from_path(&output)?;
            for processed in processed_entries {
                output.serialize(processed)?;
            }
            output.flush()?;

            let progress = ProgressBar::new(input.len() as u64);
            progress.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} ({eta})",
                    )
                    .unwrap()
                    .progress_chars("##-"),
            );
            for chunk in input.chunks(cli.rpc_parallel) {
                let mut tasks = Vec::new();
                for entry in chunk {
                    let provider = provider.clone();
                    let entry = entry.clone();
                    let progress = progress.clone();

                    tasks.push(tokio::spawn(async move {
                        let res = process_input_entry(&provider, entry).await;
                        progress.inc(1);
                        res
                    }));
                }
                let mut processed = Vec::new();
                for res in futures::future::join_all(tasks).await {
                    let res = match res? {
                        Ok(res) => res,
                        Err(e) => {
                            eprintln!("Error: {}", e);
                            continue;
                        }
                    };
                    processed.push(res);
                }
                // sort
                processed.sort_by_key(|e| e.slot);
                for processed in processed {
                    output.serialize(processed)?;
                }
                output.flush()?;
            }
            progress.finish();
        }
    }
    Ok(())
}
