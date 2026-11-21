use std::fs;
use std::io;
use std::path::Path;

use hashbrown::HashMap;
use serde::Deserialize;

use crate::csv_parse::split_fields;
use crate::hash::fast_build_hasher;
use crate::timestamp::{CLICK_REACH_MS, MS_PER_DAY, YmCache, parse_f1_sec};

#[derive(Deserialize)]
struct Manifest {
    window_start: String,
    window_end: String,
}

pub struct Lookups {
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub click_window_start_ms: i64,
    pub window_start_day: i32,
    pub num_window_days: usize,
    pub compact_mode: bool,
    pub ym_cache: YmCache,

    /// Sorted byte-wise, includes `"unknown"`.
    pub categories: Vec<String>,
    pub unknown_cat_id: u32,
    pub product_to_cat: HashMap<Box<[u8]>, u32, ahash::RandomState>,

    /// `(campaign_id, channel)`, with last entry = `("organic", "organic")`.
    pub campaigns: Vec<(String, String)>,
    pub organic_idx: u32,
    /// Only campaigns from `campaigns.csv` (never includes `"organic"`).
    pub campaign_to_idx: HashMap<Box<[u8]>, u32, ahash::RandomState>,
}

impl Lookups {
    pub fn load(data_dir: &Path) -> io::Result<Self> {
        // 1. manifest.json
        let manifest_bytes = fs::read(data_dir.join("manifest.json"))?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let window_start_ms = parse_f1_sec(manifest.window_start.as_bytes())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_start"))?
            * 1000;
        let window_end_ms = parse_f1_sec(manifest.window_end.as_bytes())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_end"))?
            * 1000;
        let click_window_start_ms = window_start_ms - CLICK_REACH_MS;

        let window_start_day = window_start_ms.div_euclid(MS_PER_DAY) as i32;
        let last_valid_ms = window_end_ms.saturating_sub(1);
        let window_last_day = last_valid_ms.div_euclid(MS_PER_DAY) as i32;
        let num_window_days = if window_end_ms > window_start_ms {
            (window_last_day - window_start_day + 1) as usize
        } else {
            0
        };

        // 2. products.csv
        let products_raw = fs::read(data_dir.join("products.csv"))?;
        let mut raw_products: Vec<(Box<[u8]>, String)> = Vec::new();
        let mut cat_set: HashMap<String, (), ahash::RandomState> =
            HashMap::with_hasher(fast_build_hasher());
        cat_set.insert("unknown".to_string(), ());

        let mut lines = products_raw.split(|&b| b == b'\n');
        let _header = lines.next();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            if let Some([pid, cat]) = split_fields::<2>(line) {
                if !pid.is_empty() {
                    let cat_str = String::from_utf8_lossy(cat).into_owned();
                    cat_set.insert(cat_str.clone(), ());
                    raw_products.push((pid.to_vec().into_boxed_slice(), cat_str));
                }
            }
        }

        let mut categories: Vec<String> = cat_set.into_keys().collect();
        // Sort byte-wise so integer cat_id order == byte-wise lexicographical order!
        categories.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        let mut cat_to_id: HashMap<&str, u32, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(categories.len(), fast_build_hasher());
        for (i, c) in categories.iter().enumerate() {
            cat_to_id.insert(c.as_str(), i as u32);
        }
        let unknown_cat_id = *cat_to_id.get("unknown").unwrap();

        let mut product_to_cat: HashMap<Box<[u8]>, u32, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(raw_products.len(), fast_build_hasher());
        for (pid, cat_str) in raw_products {
            let cid = *cat_to_id.get(cat_str.as_str()).unwrap();
            product_to_cat.insert(pid, cid);
        }
        drop(cat_to_id);

        // 3. campaigns.csv
        let campaigns_raw = fs::read(data_dir.join("campaigns.csv"))?;
        let mut campaigns: Vec<(String, String)> = Vec::new();
        let mut campaign_to_idx: HashMap<Box<[u8]>, u32, ahash::RandomState> =
            HashMap::with_hasher(fast_build_hasher());

        let mut lines = campaigns_raw.split(|&b| b == b'\n');
        let _header = lines.next();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            if let Some([cid, chan]) = split_fields::<2>(line) {
                if !cid.is_empty() {
                    let idx = campaigns.len() as u32;
                    let cid_str = String::from_utf8_lossy(cid).into_owned();
                    let chan_str = String::from_utf8_lossy(chan).into_owned();
                    campaign_to_idx.insert(cid.to_vec().into_boxed_slice(), idx);
                    campaigns.push((cid_str, chan_str));
                }
            }
        }
        let organic_idx = campaigns.len() as u32;
        campaigns.push(("organic".to_string(), "organic".to_string()));

        let span = window_end_ms.saturating_sub(window_start_ms);
        let compact_mode = span >= 0 && span <= (u32::MAX as i64) && categories.len() <= 255;
        let ym_cache = YmCache::for_window(click_window_start_ms, window_end_ms);

        Ok(Self {
            window_start_ms,
            window_end_ms,
            click_window_start_ms,
            window_start_day,
            num_window_days,
            compact_mode,
            ym_cache,
            categories,
            unknown_cat_id,
            product_to_cat,
            campaigns,
            organic_idx,
            campaign_to_idx,
        })
    }

    #[inline(always)]
    pub fn lookup_product_cat(&self, product_id: &[u8]) -> u32 {
        self.product_to_cat
            .get(product_id)
            .copied()
            .unwrap_or(self.unknown_cat_id)
    }

    #[inline(always)]
    pub fn lookup_campaign(&self, campaign_id: &[u8]) -> Option<u32> {
        self.campaign_to_idx.get(campaign_id).copied()
    }
}
