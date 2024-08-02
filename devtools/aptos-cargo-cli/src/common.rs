// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use chrono::DateTime;
use clap::Args;
use determinator::{
    rules::{DeterminatorMarkChanged, DeterminatorPostRule, DeterminatorRules, PathRule},
    Determinator, Utf8Paths0,
};
use guppy::{
    graph::{
        cargo::{CargoOptions, CargoResolverVersion},
        DependencyDirection, PackageGraph,
    },
    CargoMetadata, MetadataCommand,
};
use log::{debug, info, warn};
use reqwest::Error;
use std::{
    fs,
    io::Read,
    path::Path,
    process::{Command, Output},
};
use url::Url;

// File types in `aptos-core` that are not relevant to the rust build and test process.
// Note: this is a best effort list and will need to be updated as time goes on.
const IGNORED_DETERMINATOR_FILE_TYPES: [&str; 4] = ["*.json", "*.md", "*.yaml", "*.yml"];

// Paths in `aptos-core` that are not relevant to the rust build and test process.
// Note: this is a best effort list and will need to be updated as time goes on.
const IGNORED_DETERMINATOR_PATHS: [&str; 8] = [
    ".assets/*",
    ".github/*",
    ".vscode/*",
    "dashboards/*",
    "developer-docs-site/*",
    "docker/*",
    "scripts/*",
    "terraform/*",
];

// The maximum number of days allowed since the merge-base commit for the branch
const MAX_NUM_DAYS_SINCE_MERGE_BASE: u64 = 7;

// The delimiter used to separate the package path and the package name.
pub const PACKAGE_NAME_DELIMITER: &str = "#";

fn workspace_dir() -> String {
    let output = Command::new("cargo")
        .arg("locate-project")
        .arg("--workspace")
        .arg("--message-format=plain")
        .output()
        .unwrap()
        .stdout;
    let cargo_path = Path::new(std::str::from_utf8(&output).unwrap().trim());
    let workspace_dir = cargo_path.parent().unwrap().to_path_buf();
    workspace_dir
        .to_str()
        .expect("Failed to parse workspace directory!")
        .to_string()
}

#[derive(Args, Debug, Clone)]
pub struct SelectedPackageArgs {
    #[arg(short, long, global = true)]
    pub package: Vec<String>,
    // TODO: add changed_since
}

impl SelectedPackageArgs {
    fn compute_changed_files(&self, merge_base: &str) -> anyhow::Result<Utf8Paths0> {
        let mut command = Command::new("git");
        command.args(["diff", "-z", "--name-only"]);
        command.arg(merge_base);

        let output = command.output().map_err(|err| anyhow!("error: {}", err))?;
        if !output.status.success() {
            return Err(anyhow!("error"));
        }

        Utf8Paths0::from_bytes(output.stdout).map_err(|(_path, err)| anyhow!("{}", err))
    }

    fn git_rev_parse(&self, merge_base: &str) -> String {
        let output = Command::new("git")
            .arg("rev-parse")
            .arg(merge_base)
            .output()
            .expect("failed to execute git rev-parse");

        String::from_utf8(output.stdout)
            .expect("invalid UTF-8")
            .trim()
            .to_owned()
    }

    fn fetch_remote_metadata(&self, merge_base: &str) -> anyhow::Result<CargoMetadata> {
        // Get the local metadata file path
        let base_sha = self.git_rev_parse(merge_base);
        let file_name = format!("metadata-{}.json", base_sha);
        let dir_path = format!("{}/target/aptos-x-tool", workspace_dir(),);
        let local_file_path = format!("{}/{}", dir_path, file_name);

        // Read the metadata from the local file (if it exists)
        if let Some(metadata) = read_metadata_from_file(&local_file_path) {
            return Ok(metadata);
        }

        // Otherwise, attempt to read the metadata from the GCS bucket
        if let Some(metadata) = read_metadata_from_gcs(merge_base, file_name, &local_file_path) {
            return Ok(metadata);
        }

        // Otherwise, attempt to recompute the metadata locally
        info!("Attempting to recompute metadata locally...");
        if let Some(metadata) = recompute_merge_base_metadata(merge_base, &local_file_path) {
            return Ok(metadata);
        }

        // Otherwise, return an error
        Err(anyhow!(
            "Failed to fetch or recompute remote metadata for merge-base commit: {:?}",
            merge_base
        ))
    }

    /// Identifies the changed files compared to the merge base, and
    /// returns the relevant package graphs and file list.
    pub fn identify_changed_files(
        &self,
    ) -> anyhow::Result<(PackageGraph, PackageGraph, Utf8Paths0)> {
        // Determine the merge base
        let merge_base = self.identify_merge_base()?;

        // Download the merge base metadata
        let base_metadata = self.fetch_remote_metadata(&merge_base)?;
        let base_package_graph = base_metadata.build_graph().unwrap();

        // Compute head metadata
        let head_metadata = MetadataCommand::new()
            .exec()
            .map_err(|e| anyhow!("{}", e))?;
        let head_package_graph = head_metadata.build_graph().unwrap();

        // Compute changed files
        let changed_files = self.compute_changed_files(&merge_base)?;
        debug!("Identified the changed files: {:?}", changed_files);

        // Return the package graphs and the changed files
        Ok((base_package_graph, head_package_graph, changed_files))
    }

    /// Identifies the merge base to compare against. This is done by identifying
    /// the commit at which the current branch forked off origin/main.
    ///
    /// Note: if the merge-base is too old, an error will be returned.
    fn identify_merge_base(&self) -> anyhow::Result<String> {
        // Run the git merge-base command
        let merge_base_output = Command::new("git")
            .arg("merge-base")
            .arg("HEAD")
            .arg("origin/main")
            .output()
            .expect("failed to execute git merge-base");
        let merge_base = parse_string_from_output(merge_base_output);

        // Get the commit timestamp of the merge-base
        let commit_timestamp_output = Command::new("git")
            .arg("show")
            .arg("-s")
            .arg("--format=%cI")
            .arg(&merge_base)
            .output()
            .expect("failed to execute git show");
        let commit_timestamp = parse_string_from_output(commit_timestamp_output);

        // Log the merge-base and commit timestamp
        info!(
            "Identified the merge base: {:?} (commit date: {:?})",
            merge_base, commit_timestamp
        );

        // Calculate the time difference between the merge-base and the current time (in days)
        let commit_datetime = DateTime::parse_from_rfc3339(&commit_timestamp)
            .expect("failed to parse commit timestamp");
        let current_datetime = DateTime::parse_from_rfc3339(&chrono::Utc::now().to_rfc3339())
            .expect("failed to parse current timestamp");
        let time_difference_days = current_datetime
            .signed_duration_since(commit_datetime)
            .num_days() as u64;

        // Check if the merge-base is too old
        if time_difference_days >= MAX_NUM_DAYS_SINCE_MERGE_BASE {
            return Err(anyhow!(
                "The merge base is too old ({:?} days)! Please rebase your branch!",
                time_difference_days
            ));
        } else {
            info!(
                "The merge base is within the maximum number of days. Maximum: {:?} (days), Merge-base: {:?} (days)",
                MAX_NUM_DAYS_SINCE_MERGE_BASE, time_difference_days
            );
        }

        // Return the merge-base commit
        Ok(merge_base)
    }

    /// Computes the affected target packages based on the
    /// merge base and changed file set.
    pub fn compute_target_packages(&self) -> anyhow::Result<Vec<String>> {
        if !self.package.is_empty() {
            return Ok(self.package.clone());
        }

        // Compute changed files
        let (base_package_graph, head_package_graph, changed_files) =
            self.identify_changed_files()?;

        // Create the determinator using the package graphs
        let mut determinator = Determinator::new(&base_package_graph, &head_package_graph);

        // Add the changed files to the determinator
        determinator.add_changed_paths(&changed_files);

        // Set the cargo options for the determinator
        let mut cargo_options = CargoOptions::new();
        cargo_options.set_resolver(CargoResolverVersion::V2);
        cargo_options.set_include_dev(true); // Include dev-dependencies to ensure test-only packages are handled correctly
        determinator.set_cargo_options(&cargo_options);

        // Set the ignore rules for the determinator
        let mut rules = DeterminatorRules::default();
        for globs in [
            IGNORED_DETERMINATOR_FILE_TYPES.to_vec(),
            IGNORED_DETERMINATOR_PATHS.to_vec(),
        ] {
            rules.path_rules.push(PathRule {
                globs: globs.iter().map(|string| string.to_string()).collect(),
                mark_changed: DeterminatorMarkChanged::Packages(vec![]),
                post_rule: DeterminatorPostRule::Skip,
            });
        }
        determinator.set_rules(&rules).unwrap();

        // Run the target determinator
        let determinator_set = determinator.compute();

        // Collect the affected packages
        let package_set = determinator_set
            .affected_set
            .packages(DependencyDirection::Forward)
            .map(|package| {
                let manifest_path = package.manifest_path();
                let parent_path = manifest_path.parent().expect("must exist");
                let mut url = Url::from_directory_path(parent_path)
                    .expect("must be a valid directory path")
                    .to_string();
                if url.ends_with('/') {
                    url.pop();
                }
                format!("{}{}{}", url, PACKAGE_NAME_DELIMITER, package.name())
            })
            .collect();

        Ok(package_set)
    }
}

/// Parses the cargo metadata from the given contents
fn parse_cargo_metadata(metadata_contents: &String) -> Option<CargoMetadata> {
    match CargoMetadata::parse_json(metadata_contents) {
        Ok(cargo_metadata) => {
            // The metadata was successfully parsed from the file
            Some(cargo_metadata)
        },
        Err(error) => {
            warn!("Error parsing metadata from contents: {:?}", error);
            None
        },
    }
}

/// Parses a UTF-8 string from the given command output
fn parse_string_from_output(output: Output) -> String {
    String::from_utf8(output.stdout)
        .expect("invalid UTF-8")
        .trim()
        .to_owned()
}

/// Prints a warning message for the failed fetch of the remote Cargo.toml
fn print_cargo_toml_fetch_error(merge_base: &str, error: Error) {
    warn!(
        "Failed to fetch remote Cargo.toml for merge-base commit: {:?}! Error: {:?}\
                            \nRebasing your branch may fix this error!",
        merge_base, error
    );
}

/// Prints a warning message for the failed fetch of the remote metadata
fn print_metadata_fetch_error(merge_base: &str, error: Error) {
    warn!(
        "Failed to fetch remote metadata for merge-base commit: {:?}! Error: {:?}\
                            \nRebasing your branch may fix this error!",
        merge_base, error
    );
}

/// Attempt to read the cargo metadata from the local file (if it exists).
/// Otherwise, none is returned if the file does not exist or an error occurs.
fn read_metadata_from_file(local_file_path: &String) -> Option<CargoMetadata> {
    if let Ok(file) = fs::File::open(local_file_path) {
        // Read the file and parse the contents
        let mut contents = String::new();
        let mut buf_reader = std::io::BufReader::new(file);
        if let Err(error) = buf_reader.read_to_string(&mut contents) {
            warn!(
                "Error reading metadata file from local path: {:?}. Error: {:?}",
                local_file_path, error
            );
        } else {
            // Parse the contents as cargo metadata
            return parse_cargo_metadata(&contents);
        }
    }

    None // Unable to read the metadata from the local file
}

/// Attempt to read the cargo metadata from the GCS bucket (if it exists).
/// Otherwise, none is returned if the file does not exist or an error occurs.
fn read_metadata_from_gcs(
    merge_base: &str,
    remote_file_name: String,
    local_file_path: &String,
) -> Option<CargoMetadata> {
    // Make a request to the GCS bucket to fetch the metadata file
    let url = format!(
        "https://storage.googleapis.com/aptos-core-cargo-metadata-public/{}",
        remote_file_name
    );
    let response = match reqwest::blocking::get(url) {
        Ok(response) => response.error_for_status(),
        Err(error) => {
            print_metadata_fetch_error(merge_base, error);
            return None;
        },
    };

    // Parse the metadata response
    match response {
        Ok(response) => {
            // Get the response text
            match response.text() {
                Ok(response) => {
                    // Parse the contents as JSON
                    let cargo_metadata = parse_cargo_metadata(&response);

                    // If the metadata was successfully parsed, write it to the local directory
                    if cargo_metadata.is_some() {
                        write_content_locally(local_file_path, response);
                    }

                    // Return the metadata
                    return cargo_metadata;
                },
                Err(error) => {
                    print_metadata_fetch_error(merge_base, error);
                    return None;
                },
            }
        },
        Err(error) => {
            print_metadata_fetch_error(merge_base, error);
        },
    }

    None // Unable to read the metadata from the GCS bucket
}

/// Recomputes and returns the metadata for the merge base commit locally
fn recompute_merge_base_metadata(
    merge_base: &str,
    local_file_path: &String,
) -> Option<CargoMetadata> {
    // Download the Cargo.toml file for the merge base commit
    let cargo_toml_url = format!(
        "https://raw.githubusercontent.com/aptos-labs/aptos-core/{}/Cargo.toml",
        merge_base
    );
    let response = match reqwest::blocking::get(cargo_toml_url) {
        Ok(response) => response.error_for_status(),
        Err(error) => {
            print_cargo_toml_fetch_error(merge_base, error);
            return None;
        },
    };

    // Parse the Cargo.toml response
    let cargo_toml_content = match response {
        Ok(response) => {
            // Get the response text
            match response.text() {
                Ok(response) => response,
                Err(error) => {
                    print_cargo_toml_fetch_error(merge_base, error);
                    return None;
                },
            }
        },
        Err(error) => {
            print_cargo_toml_fetch_error(merge_base, error);
            return None;
        },
    };

    // Write the Cargo.toml file to the local directory
    let cargo_toml_file_path = format!("{}/Cargo-{}.toml", workspace_dir(), merge_base);
    write_content_locally(&cargo_toml_file_path, cargo_toml_content);

    // Recompute the metadata for the merge base commit
    let output = Command::new("cargo")
        .arg("metadata")
        .arg("--manifest-path")
        .arg(cargo_toml_file_path.clone())
        .output()
        .expect("failed to execute cargo metadata");
    let output = parse_string_from_output(output);

    // Delete the Cargo.toml file
    //if let Err(error) = fs::remove_file(cargo_toml_file_path) {
    //   warn!(
    //       "Error deleting Cargo.toml file: {:?}. Error: {:?}",
    //        cargo_toml_file_path, error
    //    );
    //}

    // Parse the metadata from the output
    let cargo_metadata = parse_cargo_metadata(&output);

    // If the metadata was successfully parsed, write it to the local directory
    if cargo_metadata.is_some() {
        write_content_locally(local_file_path, output);
    }

    // Return the metadata
    cargo_metadata
}

/// Writes the contents to the local filesystem
fn write_content_locally(local_file_path: &String, content: String) {
    // Create the directory for the file
    if let Err(error) = fs::create_dir_all("target/aptos-x-tool") {
        warn!(
            "Error creating directory for file: {:?}. Error: {:?}",
            local_file_path, error
        );
        return;
    }

    // Write the contents of the file
    if let Err(error) = fs::write(local_file_path.clone(), content) {
        warn!(
            "Error writing file: {:?}. Error: {:?}",
            local_file_path, error
        );
    }
}
