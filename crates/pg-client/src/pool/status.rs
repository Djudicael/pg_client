#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PoolStatus {
    pub idle: usize,
    pub active: usize,
    pub total_created: u64,
    pub max_size: usize,
    pub closed: bool,
}

impl PoolStatus {
    pub fn total(&self) -> usize {
        self.idle + self.active
    }

    pub fn available(&self) -> usize {
        self.max_size.saturating_sub(self.total())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_status_total() {
        let status = PoolStatus {
            idle: 3,
            active: 5,
            total_created: 10,
            max_size: 20,
            closed: false,
        };
        assert_eq!(status.total(), 8);
    }

    #[test]
    fn test_pool_status_available() {
        let status = PoolStatus {
            idle: 3,
            active: 5,
            total_created: 10,
            max_size: 20,
            closed: false,
        };
        assert_eq!(status.available(), 12);
    }

    #[test]
    fn test_pool_status_available_saturating() {
        let status = PoolStatus {
            idle: 10,
            active: 15,
            total_created: 25,
            max_size: 20,
            closed: false,
        };
        assert_eq!(status.available(), 0);
    }
}
