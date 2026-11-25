//! Work-conserving round-robin request admission across fixed upstream classes.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[derive(Clone, Copy)]
pub(crate) enum Lane {
    Txt,
    Xml,
    Catalog,
    Feed,
}

const LANES: usize = 4;

#[derive(Default)]
struct State {
    queues: [VecDeque<u64>; LANES],
    next_ticket: u64,
    next_lane: usize,
    busy: bool,
}

#[derive(Default)]
pub(crate) struct Order {
    state: Mutex<State>,
    changed: Notify,
}

impl Order {
    pub(crate) async fn wait(self: &Arc<Self>, lane: Lane) -> Turn {
        let lane = lane as usize;
        let ticket = {
            let mut state = self.state.lock().unwrap();
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            state.queues[lane].push_back(ticket);
            ticket
        };
        let mut turn = Turn {
            order: self.clone(),
            lane,
            ticket,
            admitted: false,
        };
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().unwrap();
                if !state.busy {
                    let next = (0..LANES)
                        .map(|offset| (state.next_lane + offset) % LANES)
                        .find(|&candidate| !state.queues[candidate].is_empty());
                    if next == Some(lane) && state.queues[lane].front() == Some(&ticket) {
                        state.queues[lane].pop_front();
                        state.busy = true;
                        turn.admitted = true;
                        return turn;
                    }
                }
            }
            changed.await;
        }
    }
}

pub(crate) struct Turn {
    order: Arc<Order>,
    lane: usize,
    ticket: u64,
    admitted: bool,
}

impl Drop for Turn {
    fn drop(&mut self) {
        let mut state = self.order.state.lock().unwrap();
        if self.admitted {
            state.busy = false;
            state.next_lane = (self.lane + 1) % LANES;
        } else {
            state.queues[self.lane].retain(|&ticket| ticket != self.ticket);
        }
        drop(state);
        self.order.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    #[tokio::test]
    async fn skips_idle_lanes_and_advances_after_cancellation() {
        let order = Arc::new(Order::default());
        let held = order.wait(Lane::Txt).await;
        let next_txt = order.wait(Lane::Txt);
        let next_xml = order.wait(Lane::Xml);
        let next_feed = order.wait(Lane::Feed);
        let mut context = Context::from_waker(Waker::noop());
        let mut txt = Box::pin(next_txt);
        let mut xml = Box::pin(next_xml);
        let mut feed = Box::pin(next_feed);
        assert!(txt.as_mut().poll(&mut context).is_pending());
        assert!(xml.as_mut().poll(&mut context).is_pending());
        assert!(feed.as_mut().poll(&mut context).is_pending());
        drop(xml);
        drop(held);
        assert!(txt.as_mut().poll(&mut context).is_pending());
        let Poll::Ready(held) = feed.as_mut().poll(&mut context) else {
            panic!("Feed must progress between queued TXT starts");
        };
        drop(held);
        assert!(txt.as_mut().poll(&mut context).is_ready());
    }
}
