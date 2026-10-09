use super::*;

/// Verifies batch collection and iteration preserve insertion order.
#[test]
fn batch_collection_preserves_order() {
    let batch = [1, 2, 3].into_iter().collect::<Batch<_>>();
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());
    assert_eq!(batch.as_ref(), &[1, 2, 3]);
    assert_eq!(batch.into_vec(), vec![1, 2, 3]);
}
