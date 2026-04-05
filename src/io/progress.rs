use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub struct ProgressTracker {
    pub multi: MultiProgress,
    pub bytes_bar: ProgressBar,
    pub rows_bar: ProgressBar,
}

impl ProgressTracker {
    pub fn new(total_bytes: u64, pass_label: &str) -> Self {
        let multi = MultiProgress::new();

        let bytes_bar = multi.add(ProgressBar::new(total_bytes));
        bytes_bar.set_style(
            ProgressStyle::with_template(
                "{prefix:.bold} [{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec})",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
        bytes_bar.set_prefix(format!("{} bytes", pass_label));

        let rows_bar = multi.add(ProgressBar::new_spinner());
        rows_bar.set_style(
            ProgressStyle::with_template("{prefix:.bold} {spinner} {human_pos} rows ({per_sec})")
                .unwrap(),
        );
        rows_bar.set_prefix(format!("{} rows ", pass_label));

        Self {
            multi,
            bytes_bar,
            rows_bar,
        }
    }

    pub fn inc_bytes(&self, n: u64) {
        self.bytes_bar.inc(n);
    }

    pub fn set_bytes(&self, pos: u64) {
        self.bytes_bar.set_position(pos);
    }

    pub fn inc_rows(&self, n: u64) {
        self.rows_bar.inc(n);
    }

    pub fn finish(&self) {
        self.bytes_bar.finish_with_message("done");
        self.rows_bar.finish_with_message("done");
    }
}
