#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_protocol::MessageBuffer;

fuzz_target!(|data: &[u8]| {
    // Incremental/chunked backend framing: feed the same bytes in variable-sized
    // pieces to exercise partial-message handling and internal buffer state.
    let mut buf = MessageBuffer::new();

    let mut i = 0usize;
    while i < data.len() {
        let chunk_len = usize::from(data[i]) % 32 + 1;
        let end = (i + chunk_len).min(data.len());
        let _ = buf.try_extend(&data[i..end]);

        while let Ok(Some(_msg)) = buf.next_message() {
            // consume all decoded messages available after each chunk
        }

        i = end;
    }

    while let Ok(Some(_msg)) = buf.next_message() {
        // drain any final complete messages
    }
});
