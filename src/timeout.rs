use std::ops::Add;
use std::time::{Duration, Instant};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Timeout(Instant);

impl Timeout {
    pub fn new(duration: Duration) -> Timeout {
        Timeout(Instant::now().add(duration))
    }

    pub fn time_left(self) -> Duration {
        self.0.saturating_duration_since(Instant::now())
    }

    pub fn is_done(self) -> bool {
        self.time_left() == Duration::from_secs(0)
    }
}

#[cfg(test)]
mod test {
    use std::thread::sleep;
    use std::time::Duration;

    use crate::timeout::Timeout;

    #[test]
    fn time_left() {
        let dur = Duration::from_millis(100);
        let to = Timeout::new(dur);
        assert!(to.time_left() < dur);
        sleep(dur);
        assert_eq!(to.time_left(), Duration::from_secs(0));
    }

    #[test]
    fn is_done() {
        let dur = Duration::from_millis(100);
        let to = Timeout::new(dur);
        sleep(dur);
        assert!(to.is_done());
    }

    #[test]
    fn compare_timeouts() {
        let dur1 = Duration::from_millis(100);
        let dur2 = Duration::from_millis(200);
        let to1 = Timeout::new(dur1);
        let to2 = Timeout::new(dur2);
        assert!(to1 < to2);
    }
}