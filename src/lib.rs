pub fn run() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_returns_unit() {
        assert_eq!(run(), ());
    }
}
