use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

const BATCH_SIZE: usize = 131_072;

pub fn user_daily_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

pub fn campaign_daily_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]))
}

fn user_daily_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_column_dictionary_enabled(ColumnPath::new(vec!["user_id".to_string()]), false)
        .set_max_row_group_size(512 * 1024)
        .build()
}

fn campaign_daily_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_statistics_enabled(EnabledStatistics::None)
        .build()
}

pub struct UserDailyWriter {
    path: PathBuf,
    schema: SchemaRef,
    writer: Option<ArrowWriter<File>>,
    user_ids: Vec<u8>,
    user_id_offsets: Vec<i32>,
    days: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue_cents: Vec<i64>,
    top_cat_values: Vec<u8>,
    top_cat_offsets: Vec<i32>,
    top_cat_null_count: usize,
    top_cat_valid: BooleanBufferBuilder,
    buffered: usize,
    total_written: usize,
}

impl UserDailyWriter {
    pub fn new(path: PathBuf) -> Self {
        let mut user_id_offsets = Vec::with_capacity(BATCH_SIZE + 1);
        user_id_offsets.push(0);
        let mut top_cat_offsets = Vec::with_capacity(BATCH_SIZE + 1);
        top_cat_offsets.push(0);
        Self {
            path,
            schema: user_daily_schema(),
            writer: None,
            user_ids: Vec::with_capacity(BATCH_SIZE * 12),
            user_id_offsets,
            days: Vec::with_capacity(BATCH_SIZE),
            sessions: Vec::with_capacity(BATCH_SIZE),
            events: Vec::with_capacity(BATCH_SIZE),
            revenue_cents: Vec::with_capacity(BATCH_SIZE),
            top_cat_values: Vec::with_capacity(BATCH_SIZE * 8),
            top_cat_offsets,
            top_cat_null_count: 0,
            top_cat_valid: BooleanBufferBuilder::new(BATCH_SIZE),
            buffered: 0,
            total_written: 0,
        }
    }

    #[inline(always)]
    pub fn push(
        &mut self,
        user_id: &[u8],
        trusted_utf8: bool,
        day: i32,
        sessions: i64,
        events: i64,
        revenue_cents: i64,
        top_category: Option<&str>,
    ) -> io::Result<()> {
        if trusted_utf8 {
            self.user_ids.extend_from_slice(user_id);
        } else {
            match std::str::from_utf8(user_id) {
                Ok(_) => self.user_ids.extend_from_slice(user_id),
                Err(_) => {
                    let s = String::from_utf8_lossy(user_id);
                    self.user_ids.extend_from_slice(s.as_bytes());
                }
            }
        }
        self.user_id_offsets.push(self.user_ids.len() as i32);
        self.days.push(day);
        self.sessions.push(sessions);
        self.events.push(events);
        self.revenue_cents.push(revenue_cents);
        if let Some(cat) = top_category {
            self.top_cat_values.extend_from_slice(cat.as_bytes());
            self.top_cat_offsets.push(self.top_cat_values.len() as i32);
            self.top_cat_valid.append(true);
        } else {
            self.top_cat_offsets.push(self.top_cat_values.len() as i32);
            self.top_cat_valid.append(false);
            self.top_cat_null_count += 1;
        }
        self.buffered += 1;
        if self.buffered >= BATCH_SIZE {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn ensure_writer(&mut self) -> io::Result<&mut ArrowWriter<File>> {
        if self.writer.is_none() {
            if let Some(p) = self.path.parent() {
                fs::create_dir_all(p)?;
            }
            let file = File::create(&self.path)?;
            let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(user_daily_props()))
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.writer = Some(writer);
        }
        Ok(self.writer.as_mut().unwrap())
    }

    fn flush_batch(&mut self) -> io::Result<()> {
        if self.buffered == 0 {
            return Ok(());
        }
        let uid_offsets = std::mem::take(&mut self.user_id_offsets);
        let uid_bytes = std::mem::take(&mut self.user_ids);
        let days = std::mem::take(&mut self.days);
        let sessions = std::mem::take(&mut self.sessions);
        let events = std::mem::take(&mut self.events);
        let revenue_cents = std::mem::take(&mut self.revenue_cents);
        let top_cat_offsets = std::mem::take(&mut self.top_cat_offsets);
        let top_cat_bytes = std::mem::take(&mut self.top_cat_values);

        let uid_array = unsafe {
            StringArray::new_unchecked(
                OffsetBuffer::new_unchecked(ScalarBuffer::from(uid_offsets)),
                Buffer::from(uid_bytes),
                None,
            )
        };
        let day_array = Date32Array::new(ScalarBuffer::from(days), None);
        let sess_array = Int64Array::new(ScalarBuffer::from(sessions), None);
        let ev_array = Int64Array::new(ScalarBuffer::from(events), None);
        let rev_array = Int64Array::new(ScalarBuffer::from(revenue_cents), None);

        let top_cat_nulls = if self.top_cat_null_count > 0 {
            let buf = self.top_cat_valid.finish();
            Some(NullBuffer::new(buf))
        } else {
            self.top_cat_valid.finish();
            None
        };
        let top_cat_array = unsafe {
            StringArray::new_unchecked(
                OffsetBuffer::new_unchecked(ScalarBuffer::from(top_cat_offsets)),
                Buffer::from(top_cat_bytes),
                top_cat_nulls,
            )
        };

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(uid_array),
                Arc::new(day_array),
                Arc::new(sess_array),
                Arc::new(ev_array),
                Arc::new(rev_array),
                Arc::new(top_cat_array),
            ],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        self.total_written += self.buffered;
        self.buffered = 0;
        self.top_cat_null_count = 0;
        self.user_ids.reserve(BATCH_SIZE * 12);
        self.user_id_offsets.reserve(BATCH_SIZE + 1);
        self.user_id_offsets.push(0);
        self.days.reserve(BATCH_SIZE);
        self.sessions.reserve(BATCH_SIZE);
        self.events.reserve(BATCH_SIZE);
        self.revenue_cents.reserve(BATCH_SIZE);
        self.top_cat_values.reserve(BATCH_SIZE * 8);
        self.top_cat_offsets.reserve(BATCH_SIZE + 1);
        self.top_cat_offsets.push(0);
        if self.top_cat_valid.len() > 0 {
            self.top_cat_valid = BooleanBufferBuilder::new(BATCH_SIZE);
        }

        let w = self.ensure_writer()?;
        w.write(&batch)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn finish(mut self, write_empty_if_none: bool) -> io::Result<()> {
        if self.buffered > 0 {
            self.flush_batch()?;
        }
        if self.total_written == 0 && write_empty_if_none && self.writer.is_none() {
            let _ = self.ensure_writer()?;
        }
        if let Some(w) = self.writer.take() {
            w.close()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        Ok(())
    }
}

pub fn write_empty_user_daily(path: &Path) -> io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let file = File::create(path)?;
    let schema = user_daily_schema();
    let writer = ArrowWriter::try_new(file, schema, Some(user_daily_props()))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    writer
        .close()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

#[derive(Clone, Copy)]
pub struct CampaignDailyRow {
    pub camp_idx: usize,
    pub day: i32,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

pub fn write_campaign_daily(
    dir: &Path,
    campaigns: &[(String, String)],
    rows: &[CampaignDailyRow],
) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let file = File::create(dir.join("part-000.parquet"))?;
    let schema = campaign_daily_schema();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(campaign_daily_props()))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    if !rows.is_empty() {
        let n = rows.len();
        let mut camp_bytes = Vec::with_capacity(n * 8);
        let mut camp_offsets = Vec::with_capacity(n + 1);
        camp_offsets.push(0);
        let mut chan_bytes = Vec::with_capacity(n * 8);
        let mut chan_offsets = Vec::with_capacity(n + 1);
        chan_offsets.push(0);
        let mut days = Vec::with_capacity(n);
        let mut sessions = Vec::with_capacity(n);
        let mut purch = Vec::with_capacity(n);
        let mut rev = Vec::with_capacity(n);
        let mut assisted = Vec::with_capacity(n);

        for r in rows {
            let (ref cid, ref ch) = campaigns[r.camp_idx];
            camp_bytes.extend_from_slice(cid.as_bytes());
            camp_offsets.push(camp_bytes.len() as i32);
            chan_bytes.extend_from_slice(ch.as_bytes());
            chan_offsets.push(chan_bytes.len() as i32);
            days.push(r.day);
            sessions.push(r.sessions);
            purch.push(r.purchasing_sessions);
            rev.push(r.revenue_cents);
            assisted.push(r.assisted_sessions);
        }

        let camp_array = unsafe {
            StringArray::new_unchecked(
                OffsetBuffer::new_unchecked(ScalarBuffer::from(camp_offsets)),
                Buffer::from(camp_bytes),
                None,
            )
        };
        let chan_array = unsafe {
            StringArray::new_unchecked(
                OffsetBuffer::new_unchecked(ScalarBuffer::from(chan_offsets)),
                Buffer::from(chan_bytes),
                None,
            )
        };
        let day_array = Date32Array::new(ScalarBuffer::from(days), None);
        let sess_array = Int64Array::new(ScalarBuffer::from(sessions), None);
        let purch_array = Int64Array::new(ScalarBuffer::from(purch), None);
        let rev_array = Int64Array::new(ScalarBuffer::from(rev), None);
        let assist_array = Int64Array::new(ScalarBuffer::from(assisted), None);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(camp_array),
                Arc::new(chan_array),
                Arc::new(day_array),
                Arc::new(sess_array),
                Arc::new(purch_array),
                Arc::new(rev_array),
                Arc::new(assist_array),
            ],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        writer
            .write(&batch)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    }

    writer
        .close()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}
