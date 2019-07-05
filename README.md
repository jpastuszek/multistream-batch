# Example usage

## Multiline
```sh
cat multiline1.txt | cargo run -- --pattern '^20' --stream-id-pattern '.*: ' --join " "
```

```sh
cat multiline2.txt | cargo run -- --pattern '^\s' --negate --join " "
```

```sh
cat multiline3.txt | cargo run -- --pattern '\\$' --negate --match-last --join " " --strip-pattern
```
