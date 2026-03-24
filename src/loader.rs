use std::sync::mpsc;
use arrow::array::*;
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub dtype: String,
}

#[derive(Debug)]
pub struct ParquetData {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub col_count: usize,
    pub file_size: u64,
    pub file_path: String,
}

pub enum LoadResult {
    Ok(ParquetData),
    Err(String),
}

pub fn load_async(path: String, tx: mpsc::Sender<LoadResult>) {
    std::thread::spawn(move || {
        tx.send(load_file(&path)).ok();
    });
}

fn load_file(path: &str) -> LoadResult {
    let file_size = std::fs::metadata(path)
        .map(|m| m.len())
        .unwrap_or(0);

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => return LoadResult::Err(format!("Cannot open file: {e}")),
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => return LoadResult::Err(format!("Cannot read parquet: {e}")),
    };

    let schema = builder.schema().clone();
    let reader = match builder.build() {
        Ok(r) => r,
        Err(e) => return LoadResult::Err(format!("Cannot build reader: {e}")),
    };

    let columns: Vec<ColumnMeta> = schema
        .fields()
        .iter()
        .map(|f| ColumnMeta {
            name: f.name().clone(),
            dtype: friendly_dtype(f.data_type()),
        })
        .collect();

    let col_count = columns.len();
    let mut all_rows: Vec<Vec<String>> = Vec::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => return LoadResult::Err(format!("Error reading batch: {e}")),
        };

        let n = batch.num_rows();
        for row_idx in 0..n {
            let mut row = Vec::with_capacity(col_count);
            for col_idx in 0..col_count {
                let col = batch.column(col_idx);
                row.push(format_value(col.as_ref(), row_idx));
            }
            all_rows.push(row);
        }
    }

    let row_count = all_rows.len();
    LoadResult::Ok(ParquetData {
        columns,
        rows: all_rows,
        row_count,
        col_count,
        file_size,
        file_path: path.to_string(),
    })
}

fn friendly_dtype(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float16 => "float16".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Binary | DataType::LargeBinary => "bytes".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(u, tz) => {
            let tz_str = tz.as_deref().unwrap_or("no tz");
            format!("timestamp[{u:?}, {tz_str}]")
        }
        DataType::List(f) => format!("list<{}>", friendly_dtype(f.data_type())),
        DataType::Struct(_) => "struct".into(),
        DataType::Dictionary(_, v) => format!("dict<{}>", friendly_dtype(v)),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        other => format!("{other:?}"),
    }
}

fn format_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return String::new();
    }
    use arrow::array::*;
    use arrow::datatypes::DataType::*;
    match array.data_type() {
        Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(idx).to_string()
        }
        Int8 => array.as_any().downcast_ref::<Int8Array>().unwrap().value(idx).to_string(),
        Int16 => array.as_any().downcast_ref::<Int16Array>().unwrap().value(idx).to_string(),
        Int32 => array.as_any().downcast_ref::<Int32Array>().unwrap().value(idx).to_string(),
        Int64 => array.as_any().downcast_ref::<Int64Array>().unwrap().value(idx).to_string(),
        UInt8 => array.as_any().downcast_ref::<UInt8Array>().unwrap().value(idx).to_string(),
        UInt16 => array.as_any().downcast_ref::<UInt16Array>().unwrap().value(idx).to_string(),
        UInt32 => array.as_any().downcast_ref::<UInt32Array>().unwrap().value(idx).to_string(),
        UInt64 => array.as_any().downcast_ref::<UInt64Array>().unwrap().value(idx).to_string(),
        Float32 => {
            let v = array.as_any().downcast_ref::<Float32Array>().unwrap().value(idx) as f64;
            fmt_float(v)
        }
        Float64 => {
            let v = array.as_any().downcast_ref::<Float64Array>().unwrap().value(idx);
            fmt_float(v)
        }
        Utf8 => array.as_any().downcast_ref::<StringArray>().unwrap().value(idx).to_string(),
        LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(idx).to_string(),
        Date32 => {
            let days = array.as_any().downcast_ref::<Date32Array>().unwrap().value(idx);
            format_date32(days)
        }
        Date64 => {
            let ms = array.as_any().downcast_ref::<Date64Array>().unwrap().value(idx);
            format_date64(ms)
        }
        Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let v = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(idx);
            format_timestamp_ms(v)
        }
        Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let v = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(idx);
            format_timestamp_us(v)
        }
        Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let v = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(idx);
            format_timestamp_ns(v)
        }
        Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
            let v = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(idx);
            format_timestamp_s(v)
        }
        _ => {
            // Fallback: use arrow's built-in display
            use arrow::util::display::ArrayFormatter;
            use arrow::util::display::FormatOptions;
            let opts = FormatOptions::default();
            ArrayFormatter::try_new(array, &opts)
                .map(|f| f.value(idx).to_string())
                .unwrap_or_else(|_| "<?>".to_string())
        }
    }
}

// ── Float formatting (like Python's {:.6g}) ──────────────────────────────────

fn fmt_float(v: f64) -> String {
    if v == 0.0 { return "0".to_string(); }
    let abs = v.abs();
    // Use exponential if very large or very small
    if abs >= 1e6 || abs < 1e-4 {
        // 5 decimal places in exponential = 6 sig figs
        let s = format!("{v:.5e}");
        // Clean up trailing zeros in mantissa
        if let Some(e_pos) = s.find('e') {
            let mantissa = s[..e_pos].trim_end_matches('0').trim_end_matches('.');
            let exp = &s[e_pos..];
            return format!("{mantissa}{exp}");
        }
        s
    } else {
        // Fixed: choose decimal places to show ~6 sig figs
        let mag = abs.log10().floor() as i32;
        let decimals = (5 - mag).max(0) as usize;
        let s = format!("{v:.decimals$}");
        // Trim trailing zeros
        if s.contains('.') {
            let s = s.trim_end_matches('0').trim_end_matches('.');
            s.to_string()
        } else {
            s
        }
    }
}

// ── Date/time helpers ────────────────────────────────────────────────────────

fn format_date32(days: i32) -> String {
    // days since 1970-01-01
    let epoch = 2440588i64; // Julian day of 1970-01-01
    let jd = days as i64 + epoch;
    let (y, m, d) = julian_to_ymd(jd);
    format!("{y:04}-{m:02}-{d:02}")
}

fn format_date64(ms: i64) -> String {
    format_date32((ms / 86_400_000) as i32)
}

fn format_timestamp_s(s: i64) -> String {
    let days = s.div_euclid(86_400) as i32;
    let time = s.rem_euclid(86_400);
    let date = format_date32(days);
    let h = time / 3600;
    let m = (time % 3600) / 60;
    let sec = time % 60;
    format!("{date} {h:02}:{m:02}:{sec:02}")
}

fn format_timestamp_ms(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000) as i32;
    let rem = ms.rem_euclid(86_400_000);
    let date = format_date32(days);
    let h = rem / 3_600_000;
    let m = (rem % 3_600_000) / 60_000;
    let s = (rem % 60_000) / 1_000;
    let millis = rem % 1_000;
    format!("{date} {h:02}:{m:02}:{s:02}.{millis:03}")
}

fn format_timestamp_us(us: i64) -> String {
    let days = us.div_euclid(86_400_000_000) as i32;
    let rem = us.rem_euclid(86_400_000_000);
    let date = format_date32(days);
    let h = rem / 3_600_000_000;
    let m = (rem % 3_600_000_000) / 60_000_000;
    let s = (rem % 60_000_000) / 1_000_000;
    let us_part = rem % 1_000_000;
    format!("{date} {h:02}:{m:02}:{s:02}.{us_part:06}")
}

fn format_timestamp_ns(ns: i64) -> String {
    let days = ns.div_euclid(86_400_000_000_000) as i32;
    let rem = ns.rem_euclid(86_400_000_000_000);
    let date = format_date32(days);
    let h = rem / 3_600_000_000_000;
    let m = (rem % 3_600_000_000_000) / 60_000_000_000;
    let s = (rem % 60_000_000_000) / 1_000_000_000;
    let ns_part = rem % 1_000_000_000;
    format!("{date} {h:02}:{m:02}:{s:02}.{ns_part:09}")
}

fn julian_to_ymd(jd: i64) -> (i32, u32, u32) {
    // Algorithm from https://en.wikipedia.org/wiki/Julian_day#Julian_day_number_calculation
    let l = jd + 68569;
    let n = 4 * l / 146097;
    let l = l - (146097 * n + 3) / 4;
    let i = 4000 * (l + 1) / 1461001;
    let l = l - 1461 * i / 4 + 31;
    let j = 80 * l / 2447;
    let day = l - 2447 * j / 80;
    let l = j / 11;
    let month = j + 2 - 12 * l;
    let year = 100 * (n - 49) + i + l;
    (year as i32, month as u32, day as u32)
}
