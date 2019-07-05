use cotton::prelude::*;
use multistream_batch::MultistreamBatchChannel;
use multistream_batch::Command::*;
use std::io::BufReader;
use std::time::Duration;
use regex::Regex;

/// Combine multiline log messages into single line joined with special sequence
#[derive(Debug, StructOpt)]
struct Cli {
    #[structopt(flatten)]
    logging: LoggingOpt,

    /// Regex to match stream ID if to demultiplex multiple streams
    #[structopt(long = "stream-id-pattern")]
    stream_id_pattern: Option<String>,

    /// Regex to match first or last line of a multiline message
    #[structopt(long = "pattern")]
    pattern: String,

    /// Negate the pattern
    #[structopt(long = "negate")]
    negate: bool,

    /// Match last line of multiline message instead of first
    #[structopt(long = "match-last")]
    match_last: bool,

    /// Strip matched pattern from line
    #[structopt(long = "strip-pattern")]
    strip_pattern: bool,

    /// String to join multiple lines of single message together
    #[structopt(long = "join", default_value = "/012")]
    join: String,

    /// Maximum number of lines in single message
    #[structopt(long = "max-size", default_value = "2000")]
    max_size: usize,

    /// Maximum duration single message will be collecting lines for in milliseconds
    #[structopt(long = "max-duration", default_value = "200")]
    max_duration_ms: u64,
}

fn main() {
    let args = Cli::from_args();
    init_logger(&args.logging, vec![module_path!()]);

    let stream_id_regex = args.stream_id_pattern.map(|pattern| Regex::new(&pattern).or_failed_to("compile regex for stream-id-pattern"));
    let pattern = Regex::new(&args.pattern).or_failed_to("compile regex for pattern");
    let negate = args.negate;
    let match_last = args.match_last;
    let strip_pattern = args.strip_pattern;

    let mut mbatch = MultistreamBatchChannel::with_producer_thread(args.max_size, Duration::from_millis(args.max_duration_ms), args.max_size * 2, move |sender| {
        for line in BufReader::new(std::io::stdin()).lines().or_failed_to("read lines from STDIN") {
            let (stream_id, line) = if let Some(stream_id_regex) = stream_id_regex.as_ref() {
                let stream_id = stream_id_regex.find(&line).map(|m| m.as_str().to_owned());
                let line = stream_id_regex.replace(&line, "").into_owned();
                (stream_id, line)
            } else {
                (None, line.to_owned())
            };

            let matched = pattern.is_match(&line);
            let line = if matched && strip_pattern {
                pattern.replace(&line, "").into_owned()
            } else {
                line
            };

            let matched = if negate { !matched } else { matched };

            debug!("[{:?}] ({:?}) {}", stream_id, matched, line);

            if match_last {
                sender.send(Append(stream_id.clone(), line)).unwrap();
                if matched {
                    sender.send(Drain(stream_id)).unwrap();
                }
            } else {
                if matched {
                    sender.send(Drain(stream_id.clone())).unwrap();
                }
                sender.send(Append(stream_id, line)).unwrap();
            }
        }
    });

    loop {
        match mbatch.next() {
            Ok(Some((key, lines))) => {
                if let Some(key) = key {
                    print!("{}", key);
                }
                for (i, line) in lines.enumerate() {
                    //TODO: write
                    if i > 0 {
                        print!("{}", args.join);
                    }
                    print!("{}", line);
                }
                println!();
            }
            Ok(None) => continue,
            Err(_) => break,
        }
    }
}
