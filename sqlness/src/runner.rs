// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::fs::{read_dir, OpenOptions};
use std::io::{Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use prettydiff::basic::{DiffOp, SliceChangeset};
use prettydiff::diff_lines;
use regex::Regex;
use walkdir::WalkDir;

use crate::case::TestCase;
use crate::error::{Result, SqlnessError};
use crate::{config::Config, environment::EnvController};

/// Prefix for SKIP markers in result files.
/// When comparing expected vs actual results, differences that consist solely
/// of SKIP markers are treated as "no difference" (test passes).
pub const SKIP_MARKER_PREFIX: &str = "-- SQLNESS_SKIP:";

/// The entrypoint of this crate.
///
/// To run your integration test cases, simply [`new`] a `Runner` and [`run`] it.
///
/// [`new`]: crate::Runner#method.new
/// [`run`]: crate::Runner#method.run
///
/// ```rust, ignore, no_run
/// async fn run_integration_test() {
///     let runner = Runner::new(root_path, env).await;
///     runner.run().await;
/// }
/// ```
///
/// For more detailed explaination, refer to crate level documentment.
pub struct Runner<E: EnvController> {
    config: Config,
    env_controller: E,
}

impl<E: EnvController> Runner<E> {
    pub fn new(config: Config, env_controller: E) -> Self {
        Self {
            config,
            env_controller,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let environments = self.collect_env()?;
        let mut errors = Vec::new();
        let filter = Regex::new(&self.config.env_filter)?;
        let mut runnable_environments = Vec::new();

        for env in environments {
            if filter.is_match(&env) {
                runnable_environments.push(env);
            } else {
                println!("Environment({env}) is skipped!");
            }
        }

        let mut runnable_environments = runnable_environments.into_iter();
        let mut running = FuturesUnordered::new();
        for env in runnable_environments
            .by_ref()
            .take(self.config.env_parallelism.max(1))
        {
            running.push(self.run_single_env(env));
        }

        let mut stop_scheduling = false;
        while let Some((env, run_result)) = running.next().await {
            if let Err(e) = run_result {
                println!("Environment {env} run failed, error:{e:?}.");

                if self.config.fail_fast {
                    stop_scheduling = true;
                }

                errors.push(e);
            }

            if !stop_scheduling {
                if let Some(env) = runnable_environments.next() {
                    running.push(self.run_single_env(env));
                }
            }
        }

        // only return first error
        if let Some(e) = errors.pop() {
            return Err(e);
        }

        Ok(())
    }

    async fn run_single_env(&self, env: String) -> (String, Result<()>) {
        let env_config = self.read_env_config(&env);
        let config_path = env_config.as_path();
        let config_path = if config_path.exists() {
            Some(config_path)
        } else {
            None
        };
        let parallelism = self.config.parallelism.max(1);
        println!("Creating environment {env} with parallelism: {parallelism}");

        let databases = futures::future::join_all(
            (0..parallelism).map(|id| self.env_controller.start(&env, id, config_path)),
        )
        .await;

        let run_result = self.run_env(&env, &databases).await;

        futures::future::join_all(
            databases
                .into_iter()
                .map(|db| self.env_controller.stop(&env, db)),
        )
        .await;

        (env, run_result)
    }

    fn read_env_config(&self, env: &str) -> PathBuf {
        let mut path_buf = std::path::PathBuf::new();
        path_buf.push(&self.config.case_dir);
        path_buf.push(env);
        path_buf.push(&self.config.env_config_file);

        path_buf
    }

    fn collect_env(&self) -> Result<Vec<String>> {
        let mut result = vec![];

        for dir in read_dir(&self.config.case_dir)? {
            let dir = dir?;
            if dir.file_type()?.is_dir() {
                let file_name = dir.file_name().to_str().unwrap().to_string();
                result.push(file_name);
            }
        }

        Ok(result)
    }

    async fn run_env(&self, env: &str, databases: &[E::DB]) -> Result<()> {
        let case_paths = self.collect_case_paths(env).await?;
        let start = Instant::now();

        let case_queue = Arc::new(Mutex::new(case_paths));
        let failed_cases = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));

        let mut futures = Vec::new();

        // Create futures for each database to process cases
        for (db_idx, db) in databases.iter().enumerate() {
            let case_queue = case_queue.clone();
            let failed_cases = failed_cases.clone();
            let errors = errors.clone();
            let fail_fast = self.config.fail_fast;

            futures.push(async move {
                loop {
                    // Try to get next case from the queue
                    let next_case = {
                        let mut queue = case_queue.lock().expect("Failed to lock case_queue mutex");
                        if queue.is_empty() {
                            break;
                        }
                        queue.pop().unwrap()
                    };

                    let case_name = next_case.as_os_str().to_str().unwrap().to_owned();
                    match self.run_single_case(db, &next_case).await {
                        Ok(false) => {
                            println!("[DB-{:2}] Case {} failed", db_idx, case_name);
                            failed_cases.lock().unwrap().push(case_name);
                        }
                        Ok(true) => {
                            println!("[DB-{:2}] Case {} succeeded", db_idx, case_name);
                        }
                        Err(e) => {
                            println!(
                                "[DB-{:2}] Case {} failed with error {:?}",
                                db_idx, case_name, e
                            );
                            if fail_fast {
                                errors
                                    .lock()
                                    .expect("Failed to acquire lock on errors")
                                    .push((case_name, e));
                                return;
                            }
                            errors
                                .lock()
                                .expect("Failed to acquire lock on errors")
                                .push((case_name, e));
                        }
                    }
                }
            });
        }

        futures::future::join_all(futures).await;

        println!(
            "Environment {} run finished, cost:{}ms",
            env,
            start.elapsed().as_millis()
        );

        let failed_cases = failed_cases.lock().unwrap();
        if !failed_cases.is_empty() {
            println!("Failed cases:");
            println!("{failed_cases:#?}");
        }

        let errors = errors.lock().unwrap();
        if !errors.is_empty() {
            println!("Error cases:");
            println!("{errors:#?}");
        }

        let error_count = failed_cases.len() + errors.len();
        if error_count == 0 {
            Ok(())
        } else {
            Err(SqlnessError::RunFailed { count: error_count })
        }
    }

    /// Return true when this case pass, otherwise false.
    async fn run_single_case(&self, db: &E::DB, path: &Path) -> Result<bool> {
        let case_path = path.with_extension(&self.config.test_case_extension);
        let mut case = TestCase::from_file(&case_path, &self.config)?;
        let result_path = path.with_extension(&self.config.result_extension);
        let mut result_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(&result_path)?;

        // Read old result out for compare later
        let mut old_result = String::new();
        result_file.read_to_string(&mut old_result)?;

        // Execute testcase
        let mut new_result = Cursor::new(Vec::new());
        let timer = Instant::now();
        case.execute(db, &mut new_result).await?;
        let elapsed = timer.elapsed();

        // Compare old and new result
        let new_result = String::from_utf8(new_result.into_inner()).expect("not utf8 string");
        let diff_only_skip_markers = self.are_all_changes_skip_markers_from_strings(&old_result, &new_result);

        // If diff is only SKIP markers, write back old_result to keep result file clean
        let result_to_write = if diff_only_skip_markers {
            &old_result
        } else {
            &new_result
        };

        // Truncate and write result back
        result_file.set_len(0)?;
        result_file.rewind()?;
        result_file.write_all(result_to_write.as_bytes())?;

        if let Some(diff) = self.compare(&old_result, &new_result) {
            println!("Result unexpected, path:{case_path:?}");
            println!("{diff}");
            return Ok(false);
        }

        println!(
            "Test case {:?} finished, cost: {}ms",
            path.as_os_str(),
            elapsed.as_millis()
        );

        Ok(true)
    }

    async fn collect_case_paths(&self, env: &str) -> Result<Vec<PathBuf>> {
        let mut root = PathBuf::from_str(&self.config.case_dir).unwrap();
        root.push(env);

        let filter = Regex::new(&self.config.test_filter)?;
        let test_case_extension = self.config.test_case_extension.as_str();
        let mut cases: Vec<_> = WalkDir::new(&root)
            .follow_links(self.config.follow_links)
            .into_iter()
            .filter_map(|entry| {
                entry
                    .map_or(None, |entry| Some(entry.path().to_path_buf()))
                    .filter(|path| {
                        path.extension()
                            .map(|ext| ext == test_case_extension)
                            .unwrap_or(false)
                    })
            })
            .map(|path| path.with_extension(""))
            .filter(|path| {
                let filename = path
                    .file_name()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default();
                let filename_with_env = format!("{env}:{filename}");
                filter.is_match(&filename_with_env)
            })
            .collect();

        // sort the cases in an os-independent order.
        cases.sort_by(|a, b| {
            let a_lower = a.to_string_lossy().to_lowercase();
            let b_lower = b.to_string_lossy().to_lowercase();
            a_lower.cmp(&b_lower)
        });

        Ok(cases)
    }

    /// Compare result, return None if them are the same, else return diff changes
    fn compare(&self, expected: &str, actual: &str) -> Option<String> {
        let diff = diff_lines(expected, actual);
        let diff_ops = diff.diff();

        // Check if all differences are only SKIP markers
        if self.are_all_changes_skip_markers(&diff_ops) {
            return None; // Treat as no difference
        }

        let is_different = diff_ops.iter().any(|d| !matches!(d, DiffOp::Equal(_)));
        if is_different {
            return Some(format!("{}", SliceChangeset { diff: diff_ops }));
        }

        None
    }

    /// Checks if a line is a SKIP marker
    fn is_skip_marker(line: &str) -> bool {
        line.trim().starts_with(SKIP_MARKER_PREFIX)
    }

    /// Checks if all differences are only SKIP markers
    ///
    /// Returns true if:
    /// - No differences between expected and actual, OR
    /// - All inserted/deleted/replaced lines are SKIP markers
    fn are_all_changes_skip_markers<'a>(
        &self,
        diff_ops: &'a [DiffOp<'a, &'a str>],
    ) -> bool {
        for op in diff_ops {
            match op {
                DiffOp::Equal(_) => continue,
                DiffOp::Insert(lines) | DiffOp::Remove(lines) => {
                    if !lines.iter().all(|line| Self::is_skip_marker(line)) {
                        return false;
                    }
                }
                DiffOp::Replace(_old_lines, new_lines) => {
                    if !new_lines.iter().all(|line| Self::is_skip_marker(line)) {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Convenience method to check if two strings differ only by SKIP markers
    fn are_all_changes_skip_markers_from_strings(&self, expected: &str, actual: &str) -> bool {
        let diff = diff_lines(expected, actual);
        self.are_all_changes_skip_markers(&diff.diff())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::environment::EnvController;
    use async_trait::async_trait;
    use std::fmt::Display;
    use std::path::Path;

    struct MockDatabase;

    #[async_trait]
    impl Database for MockDatabase {
        async fn query(
            &self,
            _ctx: crate::case::QueryContext,
            _query: String,
        ) -> Box<dyn Display> {
            Box::new("mock result")
        }
    }

    struct MockEnvController;

    #[async_trait]
    impl EnvController for MockEnvController {
        type DB = MockDatabase;

        async fn start(&self, _env: &str, _config: Option<&Path>) -> Self::DB {
            MockDatabase
        }

        async fn stop(&self, _env: &str, _database: Self::DB) {}
    }

    fn create_test_runner() -> Runner<MockEnvController> {
        let config = Config {
            case_dir: ".".to_string(),
            test_case_extension: "sql".to_string(),
            result_extension: "result".to_string(),
            interceptor_prefix: "-- SQLNESS".to_string(),
            env_config_file: "config.toml".to_string(),
            fail_fast: true,
            test_filter: ".*".to_string(),
            env_filter: ".*".to_string(),
            follow_links: true,
            interceptor_registry: crate::interceptor::Registry::default(),
        };
        Runner::new(config, MockEnvController)
    }

    #[test]
    fn test_is_skip_marker() {
        assert!(Runner::<MockEnvController>::is_skip_marker(
            "-- SQLNESS_SKIP: version 0.14.0 < required 0.15.0"
        ));
        assert!(Runner::<MockEnvController>::is_skip_marker(
            "  -- SQLNESS_SKIP: some reason"
        ));
        assert!(!Runner::<MockEnvController>::is_skip_marker(
            "-- SQLNESS TEMPLATE {}"
        ));
        assert!(!Runner::<MockEnvController>::is_skip_marker("SELECT 1;"));
    }

    #[test]
    fn test_are_all_changes_skip_markers_no_changes() {
        let runner = create_test_runner();
        let diff = diff_lines("SELECT 1;\n1", "SELECT 1;\n1");
        assert!(runner.are_all_changes_skip_markers(&diff.diff()));
    }

    #[test]
    fn test_are_all_changes_skip_markers_only_skip_added() {
        let runner = create_test_runner();
        let diff = diff_lines(
            "SELECT 1;\n1",
            "SELECT 1;\n1\n-- SQLNESS_SKIP: version 0.14.0 < required 0.15.0",
        );
        assert!(runner.are_all_changes_skip_markers(&diff.diff()));
    }

    #[test]
    fn test_are_all_changes_skip_markers_skip_replacing_content() {
        let runner = create_test_runner();
        let diff = diff_lines(
            "SELECT 1;\n1",
            "SELECT 1;\n-- SQLNESS_SKIP: version 0.14.0 < required 0.15.0",
        );
        assert!(runner.are_all_changes_skip_markers(&diff.diff()));
    }

    #[test]
    fn test_are_all_changes_skip_markers_real_difference() {
        let runner = create_test_runner();
        let diff = diff_lines("SELECT 1;\n1", "SELECT 1;\n2");
        assert!(!runner.are_all_changes_skip_markers(&diff.diff()));
    }

    #[test]
    fn test_are_all_changes_skip_markers_mixed_changes() {
        let runner = create_test_runner();
        let diff = diff_lines(
            "SELECT 1;\n1",
            "SELECT 1;\n2\n-- SQLNESS_SKIP: version 0.14.0 < required 0.15.0",
        );
        assert!(!runner.are_all_changes_skip_markers(&diff.diff()));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt::Display;
    use std::fs;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;

    use crate::case::QueryContext;
    use crate::config::ConfigBuilder;
    use crate::{Database, EnvController, Runner};

    #[derive(Clone)]
    struct RecordingEnvController {
        state: Arc<Mutex<RecordingState>>,
    }

    #[derive(Default)]
    struct RecordingState {
        active_starts: usize,
        max_active_starts: usize,
        active_envs: HashSet<String>,
        max_active_envs: usize,
    }

    struct RecordingDb {
        env: String,
        id: usize,
    }

    #[async_trait]
    impl EnvController for RecordingEnvController {
        type DB = RecordingDb;

        async fn start(&self, env: &str, id: usize, _config: Option<&Path>) -> Self::DB {
            {
                let mut state = self.state.lock().unwrap();
                state.active_starts += 1;
                state.max_active_starts = state.max_active_starts.max(state.active_starts);

                if id == 0 {
                    state.active_envs.insert(env.to_string());
                    state.max_active_envs = state.max_active_envs.max(state.active_envs.len());
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;

            {
                let mut state = self.state.lock().unwrap();
                state.active_starts -= 1;
            }

            RecordingDb {
                env: env.to_string(),
                id,
            }
        }

        async fn stop(&self, _env: &str, database: Self::DB) {
            if database.id == 0 {
                self.state.lock().unwrap().active_envs.remove(&database.env);
            }
        }
    }

    #[async_trait]
    impl Database for RecordingDb {
        async fn query(&self, _context: QueryContext, _query: String) -> Box<dyn Display> {
            Box::new("")
        }
    }

    #[tokio::test]
    async fn starts_databases_and_environments_in_parallel() {
        let case_dir = new_case_dir(&["env_a", "env_b"]);
        let state = Arc::new(Mutex::new(RecordingState::default()));
        let controller = RecordingEnvController {
            state: state.clone(),
        };
        let config = ConfigBuilder::default()
            .case_dir(case_dir.to_string_lossy().to_string())
            .parallelism(2)
            .env_parallelism(2)
            .build()
            .unwrap();

        Runner::new(config, controller).run().await.unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.max_active_envs, 2);
        assert_eq!(state.max_active_starts, 4);

        fs::remove_dir_all(case_dir).unwrap();
    }

    fn new_case_dir(environments: &[&str]) -> std::path::PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("sqlness-runner-test-{now}"));
        for environment in environments {
            fs::create_dir_all(dir.join(environment)).unwrap();
        }
        dir
    }
}
