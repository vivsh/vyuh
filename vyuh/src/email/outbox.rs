use super::{
    Arc, Delivery, DeliveryError, DeliveryOutcome, Future, MailDelivery, Pin, PreparedEmail,
    permanent, receipt,
};

/// A captured message and its submission metadata for integration assertions.
#[derive(Debug, Clone)]
pub struct OutboxMessage {
    message_id: String,
    source: String,
    idempotency_key: Option<String>,
}

impl OutboxMessage {
    /// Returns the generated MIME message identifier.
    pub fn message_id(&self) -> &str {
        &self.message_id
    }
    /// Returns formatted MIME source for precise test assertions.
    pub fn source(&self) -> &str {
        &self.source
    }
    /// Returns the requested submission key without including it in MIME.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }
}

/// In-memory mail capture installed while building a test site.
#[derive(Debug, Clone, Default)]
pub struct MailOutbox(Arc<parking_lot::Mutex<Vec<OutboxMessage>>>);

impl MailOutbox {
    /// Creates an empty outbox.
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns captured messages in send order.
    pub fn messages(&self) -> Vec<OutboxMessage> {
        self.0.lock().clone()
    }
    /// Returns the only captured message, if exactly one exists.
    pub fn single(&self) -> Option<OutboxMessage> {
        let messages = self.0.lock();
        if messages.len() == 1 {
            messages.first().cloned()
        } else {
            None
        }
    }
    /// Removes all captured messages.
    pub fn clear(&self) {
        self.0.lock().clear();
    }
}

pub(super) struct OutboxDelivery(pub(super) MailOutbox);

impl MailDelivery for OutboxDelivery {
    fn send(
        &self,
        mut message: PreparedEmail,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, DeliveryError>> + Send + '_>> {
        Box::pin(async move {
            let delivery = receipt(&message, DeliveryOutcome::Captured);
            let message_id = delivery.message_id.clone();
            let idempotency_key = message.idempotency_key.take();
            let message = message.into_message().map_err(permanent)?;
            self.0.0.lock().push(OutboxMessage {
                message_id,
                source: String::from_utf8_lossy(&message.formatted()).into(),
                idempotency_key,
            });
            Ok(delivery)
        })
    }

    fn outbox(&self) -> Option<MailOutbox> {
        Some(self.0.clone())
    }
}
