use std::io::BufRead;
use std::marker::Send;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

use crate::database::Database;
use crate::parser::{NestedObjectHandler, TableLocation, TableRecord};
use crate::yajlish::Parser;

type QueueItm = Option<(TableLocation, TableRecord)>;

pub struct AsyncConsumer<D: Database> {
    sender: Sender<QueueItm>,
    receiver: Receiver<QueueItm>,
    database: D,
}

impl<D: Database> AsyncConsumer<D> {
    pub fn new(database: D) -> AsyncConsumer<D> {
        let (sender, receiver) = channel();
        AsyncConsumer {
            sender,
            receiver,
            database,
        }
    }

    pub fn sender(&mut self) -> Sender<QueueItm> {
        self.sender.clone()
    }

    pub fn exhaust(&mut self) {
        loop {
            let itm = self.receiver.recv().unwrap();

            match itm {
                Some((loc, rec)) => self.database.write(loc, rec),
                None => {
                    break;
                }
            };
        }
    }
}

/// Spawns thread that will read data from 'read' and push it to sender
/// Returns immediately
pub fn consume_to_queue<B: 'static + BufRead + Send>(sender: Sender<QueueItm>, mut read: B) {
    thread::spawn(move || {
        let mut consumer = |loc: TableLocation, rec: TableRecord| {
            sender.send(Some((loc, rec))).unwrap();
        };
        let mut handler = NestedObjectHandler::new(&mut consumer);
        let mut parser = Parser::new(&mut handler);
        parser.parse(&mut read).unwrap();
        sender.send(None).unwrap();
    });
}
