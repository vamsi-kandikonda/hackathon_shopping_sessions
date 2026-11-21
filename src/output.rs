use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

const BATCH_SIZE: usize = 65_536;

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
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_column_dictionary_enabled(ColumnPath::new(vec!["user_id".to_string()]), false)
        .set_max_row_group_size(512 * 1024)
        .build()
}

fn campaign_daily_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::None)
        .build()
}

pub struct UserDailyWriter {
    path: PathBuf,
    schema: SchemaRef,
    writer: Option<ArrowWriter<File>>,
    user_ids: StringBuilder,
    days: Date32Builder,
    sessions: Int64Builder,
    events: Int64Builder,
    revenue_cents: Int64Builder,
    top_categories: StringBuilder,
    buffered: usize,
    total_written: usize,
}

impl UserDailyWriter {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            schema: user_daily_schema(),
            writer: None,
            user_ids: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12),
            days: Date32Builder::with_capacity(BATCH_SIZE),
            sessions: Int64Builder::with_capacity(BATCH_SIZE),
            events: Int64Builder::with_capacity(BATCH_SIZE),
            revenue_cents: Int64Builder::with_capacity(BATCH_SIZE),
            top_categories: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 8),
            buffered: 0,
            total_written: 0,
        }
    }

    #[inline(always)]
    pub fn push(
        &mut self,
        user_id: &[u8],
        day: i32,
        sessions: i64,
        events: i64,
        revenue_cents: i64,
        top_category: Option<&str>,
    ) -> io::Result<()> {
        match std::str::from_utf8(user_id) {
            Ok(s) => self.user_ids.append_value(s),
            Err(_) => self.user_ids.append_value(String::from_utf8_lossy(user_id)),
        }
        self.days.append_value(day);
        self.sessions.append_value(sessions);
        self.events.append_value(events);
        self.revenue_cents.append_value(revenue_cents);
        match top_category {
            Some(c) => self.top_categories.append_value(c),
            None => self.top_categories.append_null(),
        }
        self.buffered += 1;
        if self.buffered >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn ensure_writer(&mut self) -> io::Result<&mut ArrowWriter<File>> {
        if self.writer.is_none() {
            let file = File::create(&self.path)?;
            let w = ArrowWriter::try_new(file, self.schema.clone(), Some(user_daily_props()))
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.writer = Some(w);
        }
        Ok(self.writer.as_mut().unwrap())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buffered == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.user_ids.finish()),
                Arc::new(self.days.finish()),
                Arc::new(self.sessions.finish()),
                Arc::new(self.events.finish()),
                Arc::new(self.revenue_cents.finish()),
                Arc::new(self.top_categories.finish()),
            ],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let n = self.buffered;
        self.buffered = 0;
        let w = self.ensure_writer()?;
        w.write(&batch)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.total_written += n;
        Ok(())
    }

    /// Finish writing. Returns how many rows were written.
    /// If `force_create` is true, writes an empty Parquet file even if 0 rows.
    pub fn finish(mut self, force_create: bool) -> io::Result<usize> {
        self.flush()?;
        if force_create && self.writer.is_none() {
            let _ = self.ensure_writer()?;
        }
        if let Some(w) = self.writer.take() {
            w.close()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        Ok(self.total_written)
    }
}

pub fn write_empty_user_daily(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let w = UserDailyWriter::new(dir.join("part-000.parquet"));
    w.finish(true)?;
    Ok(())
}

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
    let schema = campaign_daily_schema();
    let file = File::create(dir.join("part-000.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(campaign_daily_props()))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    if !rows.is_empty() {
        let mut camp_ids = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
        let mut channels = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
        let mut days = Date32Builder::with_capacity(rows.len());
        let mut sessions = Int64Builder::with_capacity(rows.len());
        let mut purch = Int64Builder::with_capacity(rows.len());
        let mut rev = Int64Builder::with_capacity(rows.len());
        let mut assisted = Int64Builder::with_capacity(rows.len());

        for r in rows {
            let (ref cid, ref ch) = campaigns[r.camp_idx];
            camp_ids.append_value(cid);
            channels.append_value(ch);
            days.append_value(r.day);
            sessions.append_value(r.sessions);
            purch.append_value(r.purchasing_sessions);
            rev.append_value(r.revenue_cents);
            assisted.append_value(r.assisted_sessions);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(camp_ids.finish()),
                Arc::new(channels.finish()),
                Arc::new(days.finish()),
                Arc::new(sessions.finish()),
                Arc::new(purch.finish()),
                Arc::new(rev.finish()),
                Arc::new(assisted.finish()),
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
