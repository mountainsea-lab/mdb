#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerLifecycleState {
    Created,
    Initialized,
    Stopped,
}
