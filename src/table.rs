/// Virtual scrolling table state
pub struct TableState {
    pub sort_col: Option<usize>,
    pub sort_asc: bool,
    pub row_order: Vec<usize>, // indices into data.rows
}

impl TableState {
    pub fn new(row_count: usize) -> Self {
        Self {
            sort_col: None,
            sort_asc: true,
            row_order: (0..row_count).collect(),
        }
    }

    pub fn sort_by(&mut self, col: usize, rows: &[Vec<String>]) {
        if self.sort_col == Some(col) {
            self.sort_asc = !self.sort_asc;
        } else {
            self.sort_col = Some(col);
            self.sort_asc = true;
        }
        let asc = self.sort_asc;
        self.row_order.sort_by(|&a, &b| {
            let va = rows[a].get(col).map(String::as_str).unwrap_or("");
            let vb = rows[b].get(col).map(String::as_str).unwrap_or("");
            // Try numeric comparison first
            let cmp = match (va.parse::<f64>(), vb.parse::<f64>()) {
                (Ok(fa), Ok(fb)) => fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal),
                _ => va.cmp(vb),
            };
            if asc { cmp } else { cmp.reverse() }
        });
    }
}
