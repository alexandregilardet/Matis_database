#!/usr/bin/env python3

import pandas as pd
import sqlite3
import sys

conn = sqlite3.connect("salmon_v1.db")  # creates db file

c = conn.cursor()  # cursor c

# Create tables
try:
    c.executescript(
        """DROP TABLE IF EXISTS Samples;
    DROP TABLE IF EXISTS Genotypes;
    DROP TABLE IF EXISTS SNP_annotations;
    DROP TABLE IF EXISTS SNP_position;
    DROP TABLE IF EXISTS Rivers;

    CREATE TABLE Samples(
        Sample_ID text PRIMARY KEY,
        Short_ID char(12) UNIQUE, --for input into genepop max 12 char
        Species text,
        River text REFERENCES Rivers(River_short),
        Hybrid bool,
        Date_collected date,
        Latitude real,
        Longitude real
        );

    CREATE TABLE Genotypes(
        Sample_ID text REFERENCES Samples(Sample_ID),
        Probeset_ID text REFERENCES SNP_annotations(Probeset_ID),
        Genotype int NOT NULL,
        PRIMARY KEY(Sample_ID, Probeset_ID)
        );

    CREATE TABLE SNP_annotations(
        Probeset_ID text PRIMARY KEY REFERENCES SNP_position(Probeset_ID), 
        Affymetrix_ID text, 
        Flank_sequence text,
        Allele_A char(1),
        Allele_B char(1),
        Custom_ID text,
        Info text
        );

    CREATE TABLE SNP_position(
        Probeset_ID text PRIMARY KEY,
        Chromosome text,
        Position bigint
        );

    CREATE TABLE Rivers(
        River_short text PRIMARY KEY,
        River_full_name text UNIQUE,
        Country text,
        Region text,
        Longitude real,
        Latitude real
        )"""
    )

except sqlite3.Error as err:
    print(err)
    raise  # reraise after print for Python to handle

conn.commit()


def check_table_exists(tbl):
    """Check wether tbl already exists from creation of database, otherwise it would create
    an additional one from the csv indexes"""
    c.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )  # print all tables' names
    names = c.fetchall()
    print(f"Tables in the database are {names}")
    if (tbl,) in names:
        pass
    else:
        sys.exit(f"Something wrong with table {tbl}, it doesn't already exist")
    return None


def csv_into_table(filename, table):
    """Add csv into the database, csv should have columns named as the table in database,
    give table argument as a string"""
    try:
        with conn:
            csv_file = pd.read_csv(filename, sep=";")
            csv_file.to_sql(table, conn, if_exists="append", index=False)
    except sqlite3.Error:
        raise
    return None


def load_data(filenm, tb):
    check_table_exists(tb)
    csv_into_table(filenm, tb)
    return None


def get_gt_csv(file):
    """Queries and reshape to output a csv containing genotypes at each marker for 
    each individual with its short_id and population"""
    query = """SELECT Samples.Sample_ID, Samples.Short_ID, Samples.River, 
    Genotypes.Probeset_ID, Genotypes.Genotype
    FROM Samples, Genotypes
    WHERE Samples.Sample_ID = Genotypes.Sample_ID"""
    try:
        with conn:
            df = pd.read_sql_query(query, conn)
    except sqlite3.Error:
        raise
    df2 = df.pivot_table(  # long to wide format
        index=["Sample_ID", "Short_ID", "River"],
        columns="Probeset_ID",
        values="Genotype",  # str so need to adjust aggfunc
        aggfunc=lambda x: " ".join(
            x
        ),  # if several values for same index, will concatenate with space
    )
    df2.to_csv(file, sep=";")
    print(f"Pulling out all genotypes from all individuals x probes into csv {file}...")
    return None


load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/rivers/rivers_to_import_sql.csv",
    "Rivers",
)
# load_data(
# "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/snp_list/SsaTrack_Island_1-8_SNPlist_modified_sql_import.csv",
# "Snp_list",
# )
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/snp_pos/Track_SNP_Pos.csv",
    "SNP_position",
)
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/calls_mod/SsaTrack_Island_1-8.calls_mod.csv",
    "Genotypes",
)
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/samples/salmon_beta_individuals.csv",
    "Samples",
)

conn.close()

