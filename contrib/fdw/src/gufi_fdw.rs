// This file was developed from another foreign data wrapper file found in
// https://github.com/supabase/wrappers
// licensed under the Apache-2.0 license.
// https://github.com/supabase/wrappers/blob/main/LICENSE

use pgrx::PgSqlErrorCode;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::pg_sys;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Child, ChildStdout, Stdio};
use supabase_wrappers::prelude::*;

#[wrappers_fdw(
    version = "0.0.0",
    author = "GUFI Development Team",
    website = "https://github.com/mar-file-system/GUFI",
    error_type = "GufiFdwError"
)]
pub(crate) struct GufiFdw {
    gufi_query: PathBuf,
    index_root: PathBuf,
    threads: usize,
    delim: char,
    instance: Option<Child>,
    output: Option<BufReader<ChildStdout>>,
    tgt_cols: Vec<Column>,
}

impl GufiFdw {
    // https://github.com/supabase/wrappers/blob/main/wrappers/src/fdw/bigquery_fdw/bigquery_fdw.rs
    fn deparse(
        &self,
        table: String,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> String {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        };

        let mut sql = if quals.is_empty() {
            format!("select {} from {}", tgts, table)
        } else {
            let cond = quals
                .iter()
                .map(|q| q.deparse())
                .collect::<Vec<String>>()
                .join(" and ");
            format!("select {} from {} where {}", tgts, table, cond)
        };

        // push down sorts
        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| sort.deparse())
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" order by {}", order_by));
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {}", real_limit));
        }

        sql
    }
}

enum GufiFdwError {}

impl From<GufiFdwError> for ErrorReport {
    fn from(_value: GufiFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
    }
}

type GufiFdwResult<T> = Result<T, GufiFdwError>;

impl ForeignDataWrapper<GufiFdwError> for GufiFdw {
    fn new(server: ForeignServer) -> GufiFdwResult<Self> {
        Ok(Self {
            gufi_query: PathBuf::from(match server.options.get("gufi_query") {
                Some(path) => path,
                None => "gufi_query",
            }),
            index_root: match server.options.get("index_root") {
                Some(path) => PathBuf::from(path),
                None => panic!("Error: Missing index root"),
            },
            threads: if let Some(threads) = server.options.get("threads") {
                match threads.parse::<usize>() {
                    Ok(val) => val,
                    Err(msg) => panic!("Error: Bad thread count: {}: {}", threads, msg),
                }
            } else {
                1
            },
            delim: if let Some(delim) = server.options.get("delim") {
                delim.as_bytes()[0] as char
            } else {
                '|'
            },
            instance: None,
            output: None,
            tgt_cols: Vec::new(),
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> GufiFdwResult<()> {
        let sql = self.deparse(options.get("table").unwrap().clone(), quals, columns, sorts, limit);

        // set up gufi_query command
        let mut cmd = Command::new(self.gufi_query.clone());
        cmd
            .arg("-d").arg(self.delim.to_string())
            .arg("-n").arg(self.threads.to_string())
            .arg("-E").arg(sql)
            .arg(self.index_root.clone());

        // run gufi_query
        let mut instance = match cmd.stdout(Stdio::piped()).spawn() {
            Ok(process) => process,
            Err(msg)    => panic!("Error: Could not spawn gufi_query: {}", msg),
        };

        self.tgt_cols = columns.to_vec();

        self.output = match instance.stdout.take() {
            Some(stdout) => Some(BufReader::new(stdout)),
            None         => panic!("Error: Could not get stdout abc"),
        };

        self.instance = Some(instance);

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> GufiFdwResult<Option<()>> {
        let reader = match &mut self.output {
            Some(reader) => reader,
            None         => panic!("Error: Could not get output reader"),
        };

        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(len)  => {
                if len > 0 {
                    line.pop(); // remove newline
                    let cols: Vec<&str> = line.split_terminator(self.delim).collect();

                    for c in 0..self.tgt_cols.len() {
                        let name = &self.tgt_cols[c].name;
                        let col = cols[c];

                        match self.tgt_cols[c].type_oid {
                            pg_sys::INT4OID => row.push(name, Some(Cell::I32(col.parse::<i32>().unwrap()))),
                            pg_sys::TEXTOID => row.push(name, Some(Cell::String(col.to_string()))),
                            _ => todo!(),
                        };
                    }

                    // return Some(()) to Postgres and continue data scan
                    return Ok(Some(()));
                }
            }
            Err(msg) => panic!("Error: Could not read line from stdout: {}", msg),
        };

        // return 'None' to stop data scan
        Ok(None)
    }

    fn end_scan(&mut self) -> GufiFdwResult<()> {
        match &mut self.instance {
            Some(instance) => { let _ = instance.wait(); },
            None => {},
        };
        Ok(())
    }
}
