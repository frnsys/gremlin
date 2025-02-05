use std::{
    collections::BTreeMap,
    fmt::Formatter,
    sync::{Arc, Mutex},
};

use tracing::{
    field::{Field, Visit},
    span::Id,
    Level, Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

const RESET: &str = "\x1b[0m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const BLUE: &str = "\x1b[34m";

/// A tracing layer which counts matching messages.
#[derive(Default)]
pub struct CountLayer {
    messages: Arc<Mutex<BTreeMap<String, usize>>>,
}
impl CountLayer {
    pub fn counts(&self) -> BTreeMap<String, usize> {
        self.messages.lock().unwrap().clone()
    }
}
impl<S> Layer<S> for CountLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        if let Some(message) = visitor.message {
            if let Ok(mut messages) = self.messages.lock() {
                let count = messages.entry(message).or_default();
                *count += 1;
            }
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}
impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.into());
        }
    }
}

#[derive(Debug)]
struct RepeatMessage {
    message: String,
    count: usize,
    level: Level,
}
impl std::fmt::Display for RepeatMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let level_color = match self.level {
            Level::ERROR => RED,
            Level::WARN => YELLOW,
            Level::INFO => GREEN,
            Level::DEBUG => BLUE,
            Level::TRACE => RESET,
        };

        let label = format!("{}{}{}", level_color, self.level, RESET);
        let message = if self.count > 1 {
            format!("{} {} [{}x]", label, self.message, self.count)
        } else {
            format!("{} {}", label, self.message)
        };
        write!(f, "{}", message)
    }
}

/// Tracing layer that collapses consecutive duplicate messages.
#[derive(Default)]
pub struct DedupLayer {
    last_message: Arc<Mutex<Option<RepeatMessage>>>,
}

impl<S> Layer<S> for DedupLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        if let Some(message) = visitor.message {
            if let Ok(mut last) = self.last_message.lock() {
                match last.as_mut() {
                    Some(last_) => {
                        if last_.message == message {
                            last_.count += 1;
                        } else {
                            println!("{}", last_);
                            last_.message = message;
                            last_.level = *event.metadata().level();
                            last_.count = 1;
                        }
                    }
                    None => {
                        *last = Some(RepeatMessage {
                            message,
                            level: *event.metadata().level(),
                            count: 1,
                        })
                    }
                }
            }
        }
    }

    fn on_close(&self, _id: Id, _ctx: Context<'_, S>) {
        if let Ok(mut last) = self.last_message.lock() {
            if let Some(message) = last.as_ref() {
                println!("{}", message);
                *last = None;
            }
        }
    }
}
