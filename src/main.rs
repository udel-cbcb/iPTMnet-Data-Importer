extern crate clap;
extern crate postgres;
extern crate simplelog;

#[macro_use]
extern crate log;

use postgres::{Connection, TlsMode};
use simplelog::*;
use std::fs::File;
use clap::{App, Arg};

fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Info, Config::default()).unwrap(),
    ]).unwrap();

    let matches = App::new("iPTMnet data importer")
        .version("1.0")
        .author("Sachin Gavali. <saching@ude.edu>")
        .about("Imports data from CSV files into postgres database")
        .arg(
            Arg::with_name("HOST")
                .long("host")
                .help("The address of postgres database. Default - localhost")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("PORT")
                .long("port")
                .help("The port on which postgres database is running. Default - 5432 ")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("USERNAME")
                .long("user")
                .help("Username of the user that owns iptmnet database. Default - postgres")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("PASSWORD")
                .long("pass")
                .help("Password of the user that owns iptmnet database. Default - postgres")
                .takes_value(true),
        )
        .get_matches();

    let mut username = "postgres";
    let mut password = "postgres";
    let mut host = "localhost";
    let mut port = "5432";
    let database = "iptmnet";

    if matches.is_present("HOST") {
        host = matches.value_of("HOST").unwrap();
    }

    if matches.is_present("PORT") {
        port = matches.value_of("PORT").unwrap();
    }

    if matches.is_present("USERNAME") {
        username = matches.value_of("USERNAME").unwrap();
    }

    if matches.is_present("PASSWORD") {
        password = matches.value_of("PASSWORD").unwrap();
    }

    let connection_string = format!(
        "postgres://{username}:{password}@{host}:{port}/{database}",
        username = username,
        password = password,
        host = host,
        port = port,
        database = database
    );

    info!(
        "{}",
        format!(
            "Connecting to database at - {}.",
            connection_string.as_str()
        )
    );

    let conn;
    let conn_result = Connection::connect(connection_string.as_str(), TlsMode::None);
    match conn_result {
        Ok(value) => {
            conn = value;
        },
        Err(error) => {
            error!("{}", error);
            std::process::exit(1);
        }
    }


    //START the transaction
    let start_transaction_result = conn.execute("BEGIN;", &[]);
    match start_transaction_result {
        Ok(_) => info!("STARTED TRANSACTION"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    //drop table MV_ENTRY
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_ENTRY;", &[]);
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_ENTRY"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    //drop table MV_EVENT
    let drop_mv_event_result = conn.execute("DROP TABLE IF EXISTS MV_EVENT;", &[]);
    match drop_mv_event_result {
        Ok(_) => info!("DROPPED TABLE MV_EVENT"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    //drop table MV_EFIP
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_EFIP;", &[]);
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_EFIP"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }


    //drop table MV_EFIP
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_PROTEO;", &[]);
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_PROTEO"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    
    //create table MV_ENTRY
    create_mv_entry(&conn);

    //create table MV_EVENT
    create_mv_event(&conn);

    //create table MV_EFIP
    create_mv_efip(&conn);

    //create table MV_PROTEO
    create_mv_proteo(&conn);

    //populate mv_entry
    populate_mv_entry(&conn);

    //populate mv event
    populate_mv_event(&conn);

    //populate mv efip
    populate_mv_efip(&conn);
    
    //populate mv_proteo
    populate_mv_proteo(&conn);

    //END the transaction
    let end_transaction_result = conn.execute("COMMIT;", &[]);
    match end_transaction_result {
        Ok(_) => info!("END TRANSACTION"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn create_mv_entry(conn: &Connection) {
    let create_mv_entry_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_ENTRY
        (
            IPTM_ENTRY_ID BIGINT NOT NULL,
            IPTM_ENTRY_CODE VARCHAR(25) NOT NULL,
            IPTM_ENTRY_TYPE VARCHAR(10) NOT NULL,
            IPTM_ENTRY_SYMBOL VARCHAR(4000),
            UNIPROT_ID VARCHAR(50),
            PROTEIN_NAME VARCHAR(200),
            GENE_NAME VARCHAR(50),
            PROTEIN_SYNONYMS TEXT,
            GENE_SYNONYMS TEXT,
            DEFINITION TEXT,
            CATEGORY VARCHAR(25),
            IS_REVIEWED CHAR(1),
            TAXON_CODE VARCHAR(25),
            TAXON_SPECIES VARCHAR(200),
            TAXON_COMMON VARCHAR(100),
            NOTE TEXT,
            SITES TEXT,
            XREF VARCHAR(25),
            NUM_ENZYME BIGINT,
            NUM_SUBSTRATE BIGINT,
            NUM_PPI BIGINT,
            NUM_SITE BIGINT,
            NUM_FORM BIGINT,
            ROLE_AS_ENZYME CHAR(1),
            ROLE_AS_SUBSTRATE CHAR(1),
            ROLE_AS_PPI CHAR(1),
            WEIGHT BIGINT,
            LIST_AS_SUBSTRATE VARCHAR(25),
            LIST_AS_ENZYME VARCHAR(25),
            HAS_OVERLAP_PTM CHAR(1),
            PROTEIN_SYN VARCHAR(4000),
            GENE_SYN VARCHAR(4000)
        )",
        &[],
    );

    match create_mv_entry_result {
        Ok(_) => info!("CREATED TABLE MV_ENTRY"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_entry(conn: &Connection) {
    // Read mv_entry_exported.csv
    let mut file;
    match File::open("./mv_entry_export.csv") {
        Ok(value) => {
            file = value;
        },
        Err(error) => {
            error!{"{}",error};
            std::process::exit(-1);
        }
    }

    info!("POPULATING MV_ENTRY");
    

    let stmt_result = conn.prepare("COPY mv_entry FROM STDIN DELIMITER ',' CSV HEADER");
    let stmt;
    match stmt_result {
        Ok(value) => {
            stmt = value;
        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }

    let copy_result = stmt.copy_in(&[], &mut file);
    match copy_result {
        Ok(_) => {

        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }

}

fn create_mv_event(conn: &Connection) {
    let create_mv_event_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_EVENT
        (
            IPTM_EVENT_ID BIGINT NOT NULL,
            SUB_FORM_CODE VARCHAR(25),
            SUB_CODE VARCHAR(25),
            SUB_TYPE VARCHAR(10),
            SUB_UNIPROT_ID VARCHAR(50),
            SUB_SYMBOL VARCHAR(4000),
            SUB_TAXON_CODE VARCHAR(25),
            SUB_TAXON_COMMON VARCHAR(100),
            SUB_SITES TEXT,
            SUB_XREF VARCHAR(25),
            ENZ_FORM_CODE VARCHAR(25),
            ENZ_CODE VARCHAR(25),
            ENZ_TYPE VARCHAR(10),
            ENZ_UNIPROT_ID VARCHAR(50),
            ENZ_SYMBOL VARCHAR(4000),
            ENZ_TAXON_CODE VARCHAR(25),
            ENZ_TAXON_COMMON VARCHAR(100),
            ENZ_SITES TEXT,
            ENZ_XREF VARCHAR(25),
            EVENT_NAME VARCHAR(50),
            EVENT_LABEL VARCHAR(10),
            SOURCE_LABEL VARCHAR(10),
            IS_AUTO_GENERATED CHAR(1),
            RESIDUE VARCHAR(1),
            POSITION BIGINT,
            MODIFIER VARCHAR(50),
            NOTE TEXT,
            PMIDS TEXT,
            NUM_SUBSTRATES VARCHAR(4000)
        )",
        &[],
    );

    match create_mv_event_result {
        Ok(_) => info!("CREATED TABLE MV_EVENT"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_event(conn: &Connection) {
    // Read mv_event_exported.csv
    let mut file;
    match File::open("./mv_event_export.csv") {
        Ok(value) => {
            file = value;
        },
        Err(error) => {
            error!{"{}",error};
            std::process::exit(-1);
        }
    }
    
    info!("POPULATING MV_EVENT");

    let stmt_result = conn.prepare("COPY mv_event FROM STDIN DELIMITER ',' CSV HEADER");
    let stmt;
    match stmt_result {
        Ok(value) => {
            stmt = value;
        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }

    let copy_result = stmt.copy_in(&[], &mut file);
    match copy_result {
        Ok(_) => {

        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }
    
}

fn create_mv_efip(conn: &Connection) {
    let create_mv_efip_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_EFIP
        (
            PPI_EVENT_ID BIGINT,
            PTM_EVENT_ID BIGINT,
            IMPACT VARCHAR(50),
            PPI_SUB_CODE VARCHAR(25),
            PPI_SUB_TYPE VARCHAR(10),
            PPI_SUB_SYMBOL VARCHAR(4000),
            PPI_SUB_TAXON_CODE VARCHAR(25),
            PPI_SUB_TAXON_COMMON VARCHAR(100),
            PPI_SUB_SITES TEXT,
            PPI_PR_CODE VARCHAR(25),
            PPI_PR_TYPE VARCHAR(10),
            PPI_PR_SYMBOL VARCHAR(4000),
            PPI_PR_TAXON_CODE VARCHAR(25),
            PPI_PR_TAXON_COMMON VARCHAR(100),
            PPI_SOURCE_LABEL VARCHAR(10),
            PPI_NOTE TEXT,
            PPI_PMIDS TEXT,
            PTM_SUB_CODE VARCHAR(25),
            PTM_SUB_TYPE VARCHAR(10),
            PTM_SUB_SYMBOL VARCHAR(4000),
            PTM_SUB_TAXON_CODE VARCHAR(25),
            PTM_SUB_TAXON_COMMON VARCHAR(100),
            PTM_SUB_SITES TEXT,
            PTM_ENZ_CODE VARCHAR(25),
            PTM_ENZ_TYPE VARCHAR(10),
            PTM_ENZ_SYMBOL VARCHAR(4000),
            PTM_ENZ_TAXON_CODE VARCHAR(25),
            PTM_ENZ_TAXON_COMMON VARCHAR(100),
            PTM_EVENT_NAME VARCHAR(50),
            PTM_EVENT_LABEL VARCHAR(10),
            PTM_RESIDUE VARCHAR(1),
            PTM_POSITION BIGINT,
            PTM_SOURCE_LABEL VARCHAR(10),
            PTM_NOTE TEXT,
            PTM_PMIDS TEXT
        )",
        &[],
    );

    match create_mv_efip_result {
        Ok(_) => info!("CREATED TABLE MV_EFIP"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_efip(conn: &Connection) {
    // Read mv_efip_exported.csv
    let mut file;
    match File::open("./mv_efip_export.csv") {
        Ok(value) => {
            file = value;
        },
        Err(error) => {
            error!{"{}",error};
            std::process::exit(-1);
        }
    }
 
    info!("POPULATING MV_EFIP");

    let stmt_result = conn.prepare("COPY mv_efip FROM STDIN DELIMITER ',' CSV HEADER");
    let stmt;
    match stmt_result {
        Ok(value) => {
            stmt = value;
        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }

    let copy_result = stmt.copy_in(&[], &mut file);
    match copy_result {
        Ok(_) => {

        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }

}

fn create_mv_proteo(conn: &Connection) {
    let create_mv_entry_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_PROTEO
        (
            SUB_CODE VARCHAR(25),
            SUB_TYPE VARCHAR(10),
            SUB_SYMBOL VARCHAR(4000),
            SUB_SITES VARCHAR(4000),
            SUB_XREF VARCHAR(25),
            ENZ_CODE VARCHAR(25),
            ENZ_TYPE VARCHAR(10),
            ENZ_SYMBOL VARCHAR(4000),
            ENZ_SITES VARCHAR(4000),
            ENZ_XREF VARCHAR(25),
            SITES VARCHAR(4000),
            EVENT_NAME VARCHAR(50) NOT NULL,
            EVENT_LABEL VARCHAR(10) NOT NULL,
            SOURCE_LABEL VARCHAR(10) NOT NULL,
            IS_AUTO_GENERATED CHAR(1) NOT NULL,
            MODIFIER VARCHAR(50),
            PMIDS VARCHAR(4000)
        )",
        &[],
    );

    match create_mv_entry_result {
        Ok(_) => info!("CREATED TABLE MV_PROTEO"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_proteo(conn: &Connection) {
    // Read mv_proteo_exported.csv
    let mut file;
    match File::open("./mv_proteo_export.csv") {
        Ok(value) => {
            file = value;
        },
        Err(error) => {
            error!{"{}",error};
            std::process::exit(-1);
        }
    }

    info!("POPULATING MV_PROTEO");

    let stmt_result = conn.prepare("COPY mv_proteo FROM STDIN DELIMITER ',' CSV HEADER");
    let stmt;
    match stmt_result {
        Ok(value) => {
            stmt = value;
        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }

    let copy_result = stmt.copy_in(&[], &mut file);
    match copy_result {
        Ok(_) => {

        },
        Err(error) => {
            error!("{}",error);
            std::process::exit(-1);
        }
    }
}
