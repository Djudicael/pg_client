#![no_main]
use libfuzzer_sys::fuzz_target;
use pg_protocol::MessageBuffer;

fuzz_target!(|data: &[u8]| {
    // Feed data into a MessageBuffer and try to decode messages.
    // This tests the buffer management under fuzz input.
    let mut buf = MessageBuffer::new();
    buf.extend(data);
    while let Ok(Some(_msg)) = buf.next_message() {
        // consume all messages
    }
});
