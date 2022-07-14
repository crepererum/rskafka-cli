//! CLI command to consume data from Kafka.
use std::{collections::BTreeMap, pin::Pin, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use futures::TryStreamExt;
use rskafka::{
    client::{
        consumer::{StartOffset, StreamConsumerBuilder},
        Client,
    },
    record::{Record, RecordAndOffset},
};
use serde::{Serialize, Serializer};
use time::OffsetDateTime;
use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
};
use tracing::info;

/// Start offset settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgStartOffset {
    /// At the earlist known offset.
    ///
    /// This might be larger than 0 if some records were already deleted due to a retention policy.
    Earliest,

    /// At the latest known offset.
    ///
    /// This is helpful if you only want ot process new data.
    Latest,

    /// At a specific offset.
    At(i64),
}

impl FromStr for ArgStartOffset {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            _ => match s.parse::<i64>() {
                Ok(o) => Ok(Self::At(o)),
                Err(_) => Err(format!("Unknown start offset: {s}")),
            },
        }
    }
}

impl From<ArgStartOffset> for StartOffset {
    fn from(so: ArgStartOffset) -> Self {
        match so {
            ArgStartOffset::Earliest => Self::Earliest,
            ArgStartOffset::Latest => Self::Latest,
            ArgStartOffset::At(o) => Self::At(o),
        }
    }
}

/// Config for data consumption.
#[derive(Debug, Parser)]
pub struct ConsumeCLIConfig {
    /// Topic.
    #[clap(long)]
    topic: String,

    /// Partition.
    #[clap(long)]
    partition: i32,

    /// Min batch size.
    #[clap(long)]
    min_batch_size: Option<i32>,

    /// Max batch size.
    #[clap(long)]
    max_batch_size: Option<i32>,

    /// Max wait time in ms.
    #[clap(long, value_parser=humantime::parse_duration)]
    max_wait_time: Option<Duration>,

    /// Start offset.
    ///
    /// Use "earliest", "latest", or an integer.
    #[clap(long, default_value = "earliest")]
    start_offset: ArgStartOffset,

    /// End offset, inclusive.
    #[clap(long)]
    end_offset: Option<i64>,

    /// Output file.
    ///
    /// Use `-` to write to stdout.
    #[clap(long)]
    output: Option<String>,
}

/// Consume data.
pub async fn consume(client: Client, config: ConsumeCLIConfig) -> Result<()> {
    let client = Arc::new(client.partition_client(config.topic, config.partition)?);

    let mut consumer_builder = StreamConsumerBuilder::new(client, config.start_offset.into());
    if let Some(min_batch_size) = config.min_batch_size {
        consumer_builder = consumer_builder.with_min_batch_size(min_batch_size);
    }
    if let Some(max_batch_size) = config.max_batch_size {
        consumer_builder = consumer_builder.with_max_batch_size(max_batch_size);
    }
    if let Some(max_wait_time) = config.max_wait_time {
        let max_wait_time_ms: i32 = max_wait_time.as_millis().try_into()?;
        consumer_builder = consumer_builder.with_max_wait_ms(max_wait_time_ms);
    }
    let mut consumer = consumer_builder.build();

    let mut out = match config.output {
        Some(out) => Some(open_output(&out).await?),
        None => None,
    };

    while let Some((record_and_offset, high_watermark)) = consumer.try_next().await? {
        let RecordAndOffset { record, offset } = record_and_offset;

        // exit early in case of offset gaps
        if config
            .end_offset
            .map(|end_offset| offset > end_offset)
            .unwrap_or_default()
        {
            break;
        }

        info!(
            high_watermark,
            offset = offset,
            record_size = record.approximate_size(),
            "got record",
        );

        let record = ConsumedRecord::new(offset, record);
        if let Some(out) = out.as_mut() {
            let mut s = serde_json::to_string(&record)?;
            s.push('\n');
            out.write_all(s.as_bytes()).await?;
            out.flush().await?;
        }

        // exit if this record was the last and avoid pulling another one
        if config
            .end_offset
            .map(|end_offset| offset == end_offset)
            .unwrap_or_default()
        {
            break;
        }
    }

    Ok(())
}

/// A machine-readable representation of a consumed record.
#[derive(Debug, Serialize)]
struct ConsumedRecord {
    /// Offset of this record.
    offset: i64,

    /// Record key.
    #[serde(serialize_with = "as_optional_base64")]
    key: Option<Vec<u8>>,

    /// Record value.
    #[serde(serialize_with = "as_optional_base64")]
    value: Option<Vec<u8>>,

    /// Headers attached to this record.
    #[serde(serialize_with = "as_base64_values")]
    headers: BTreeMap<String, Vec<u8>>,

    /// Timestamp as reported by Kafka.
    #[serde(with = "time::serde::rfc3339")]
    timestamp: OffsetDateTime,

    /// Estimated uncompressed record size.
    size: usize,

    /// Timestamp when the record was received by the client.
    #[serde(with = "time::serde::rfc3339")]
    received_at: OffsetDateTime,
}

impl ConsumedRecord {
    /// Converts record into machine-readable form.
    fn new(offset: i64, record: Record) -> Self {
        let size = record.approximate_size();
        let received_at = OffsetDateTime::now_utc();
        Self {
            offset,
            key: record.key,
            value: record.value,
            headers: record.headers,
            timestamp: record.timestamp,
            size,
            received_at,
        }
    }
}

/// Open output stream.
async fn open_output(out: &str) -> Result<Pin<Box<dyn AsyncWrite>>> {
    if out == "-" {
        Ok(Box::pin(BufWriter::new(tokio::io::stdout())))
    } else {
        Ok(Box::pin(BufWriter::new(File::create(out).await?)))
    }
}

/// Helper to write the Kafka header map with base64 values.
fn as_base64_values<S>(
    map: &BTreeMap<String, Vec<u8>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    struct Base64Wrapper<'a>(&'a [u8]);

    impl<'a> Serialize for Base64Wrapper<'a> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            as_base64(&self.0, serializer)
        }
    }

    serializer.collect_map(map.iter().map(|(k, v)| (k, Base64Wrapper(v))))
}

/// Helper to write optional bytes as base64.
fn as_optional_base64<T, S>(
    bytes: &Option<T>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: Serializer,
{
    match bytes {
        None => <Option<String>>::serialize(&None, serializer),
        Some(bytes) => as_base64(bytes, serializer),
    }
}

/// Helper to write bytes as base64.
fn as_base64<T, S>(bytes: &T, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: Serializer,
{
    serializer.collect_str(&base64::display::Base64Display::with_config(
        bytes.as_ref(),
        base64::STANDARD,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_offset_parsing() {
        assert_eq!(
            ArgStartOffset::from_str("earliest").unwrap(),
            ArgStartOffset::Earliest,
        );
        assert_eq!(
            ArgStartOffset::from_str("latest").unwrap(),
            ArgStartOffset::Latest,
        );
        assert_eq!(
            ArgStartOffset::from_str("0").unwrap(),
            ArgStartOffset::At(0),
        );
        assert_eq!(
            ArgStartOffset::from_str("1337").unwrap(),
            ArgStartOffset::At(1337),
        );
        assert_eq!(
            ArgStartOffset::from_str("EARLIEST").unwrap(),
            ArgStartOffset::Earliest,
        );
        assert_eq!(
            ArgStartOffset::from_str("EarlIEST").unwrap(),
            ArgStartOffset::Earliest,
        );
        assert_eq!(
            ArgStartOffset::from_str("").unwrap_err(),
            "Unknown start offset: ",
        );
        assert_eq!(
            ArgStartOffset::from_str("Foo").unwrap_err(),
            "Unknown start offset: Foo",
        );
    }

    #[test]
    fn test_json_serialization() {
        let record = ConsumedRecord {
            offset: 1337,
            key: None,
            value: None,
            headers: Default::default(),
            timestamp: OffsetDateTime::from_unix_timestamp(42).unwrap(),
            received_at: OffsetDateTime::from_unix_timestamp(1234567890).unwrap(),
            size: 10,
        };
        let actual = serde_json::to_string_pretty(&record).unwrap();
        let expected = r#"{
  "offset": 1337,
  "key": null,
  "value": null,
  "headers": {},
  "timestamp": "1970-01-01T00:00:42Z",
  "size": 10,
  "received_at": "2009-02-13T23:31:30Z"
}"#;
        assert_eq!(actual, expected);

        let record = ConsumedRecord {
            offset: 1337,
            key: Some(b"foo".to_vec()),
            value: Some(b"bar".to_vec()),
            headers: BTreeMap::from([("hello".to_string(), b"world".to_vec())]),
            timestamp: OffsetDateTime::from_unix_timestamp(42).unwrap(),
            received_at: OffsetDateTime::from_unix_timestamp(1234567890).unwrap(),
            size: 10,
        };
        let actual = serde_json::to_string_pretty(&record).unwrap();
        let expected = r#"{
  "offset": 1337,
  "key": "Zm9v",
  "value": "YmFy",
  "headers": {
    "hello": "d29ybGQ="
  },
  "timestamp": "1970-01-01T00:00:42Z",
  "size": 10,
  "received_at": "2009-02-13T23:31:30Z"
}"#;
        assert_eq!(actual, expected);
    }
}
