use super::*;

/// Verifies the local bucket starts at its burst and replenishes against monotonic time.
#[test]
fn local_bucket_refills_without_store_time() {
    let now = tokio::time::Instant::now();
    let mut bucket = LocalRateBucket::new(TaskRate::per_second(2).burst(2), now);
    assert_eq!(bucket.available(now), 2);
    bucket.consume(2, now);
    assert_eq!(bucket.available(now), 0);
    assert_eq!(bucket.next_permit(now), Some(Duration::from_millis(500)));

    let later = now + Duration::from_millis(500);
    assert_eq!(bucket.available(later), 1);
    bucket.consume(1, later);
    assert_eq!(bucket.next_permit(later), Some(Duration::from_millis(500)));
}

/// Verifies consuming fewer claimed rows preserves unused local burst permits.
#[test]
fn local_bucket_consumes_actual_claims_only() {
    let now = tokio::time::Instant::now();
    let mut bucket = LocalRateBucket::new(TaskRate::per_minute(10).burst(5), now);
    bucket.consume(2, now);
    assert_eq!(bucket.available(now), 3);
    assert_eq!(bucket.next_permit(now), None);
}
