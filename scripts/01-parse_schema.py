#!/usr/bin/env python3
"""
Database Schema to Neo4j Knowledge Graph Parser

This script extracts database schema information from PostgreSQL and creates
a knowledge graph in Neo4j with tables, columns, foreign keys, constraints,
and indexes as nodes with meaningful relationships.
"""

import os
import sys
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TableInfo:
    """Information about a database table."""
    schema_name: str
    table_name: str
    table_type: str
    comment: Optional[str] = None


@dataclass
class ColumnInfo:
    """Information about a table column."""
    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str]
    ordinal_position: int
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    comment: Optional[str] = None


@dataclass
class ForeignKeyInfo:
    """Information about foreign key relationships."""
    constraint_name: str
    source_schema: str
    source_table: str
    source_column: str
    target_schema: str
    target_table: str
    target_column: str
    match_option: str
    update_rule: str
    delete_rule: str


@dataclass
class ConstraintInfo:
    """Information about table constraints."""
    constraint_name: str
    schema_name: str
    table_name: str
    constraint_type: str
    column_names: List[str]
    check_clause: Optional[str] = None


@dataclass
class IndexInfo:
    """Information about table indexes."""
    schema_name: str
    table_name: str
    index_name: str
    is_unique: bool
    is_primary: bool
    column_names: List[str]
    index_type: str


class DatabaseSchemaExtractor:
    """Extracts schema information from PostgreSQL database."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
        self.connection = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL database")
    
    def extract_tables(self) -> List[TableInfo]:
        """Extract table information."""
        query = """
        SELECT 
            table_schema,
            table_name,
            table_type,
            obj_description(pgc.oid) as comment
        FROM information_schema.tables t
        LEFT JOIN pg_class pgc ON pgc.relname = t.table_name
        LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace 
            AND pgn.nspname = t.table_schema
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY table_schema, table_name;
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
        tables = [
            TableInfo(
                schema_name=row['table_schema'],
                table_name=row['table_name'],
                table_type=row['table_type'],
                comment=row['comment']
            )
            for row in results
        ]
        
        logger.info(f"Extracted {len(tables)} tables")
        return tables
    
    def extract_columns(self) -> List[ColumnInfo]:
        """Extract column information."""
        query = """
        SELECT 
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable = 'YES' as is_nullable,
            c.column_default,
            c.ordinal_position,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            col_description(pgc.oid, c.ordinal_position) as comment
        FROM information_schema.columns c
        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
        LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace 
            AND pgn.nspname = c.table_schema
        WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY c.table_schema, c.table_name, c.ordinal_position;
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
        columns = [
            ColumnInfo(
                schema_name=row['table_schema'],
                table_name=row['table_name'],
                column_name=row['column_name'],
                data_type=row['data_type'],
                is_nullable=row['is_nullable'],
                column_default=row['column_default'],
                ordinal_position=row['ordinal_position'],
                character_maximum_length=row['character_maximum_length'],
                numeric_precision=row['numeric_precision'],
                numeric_scale=row['numeric_scale'],
                comment=row['comment']
            )
            for row in results
        ]
        
        logger.info(f"Extracted {len(columns)} columns")
        return columns
    
    def extract_foreign_keys(self) -> List[ForeignKeyInfo]:
        """Extract foreign key relationships."""
        query = """
        SELECT 
            tc.constraint_name,
            tc.table_schema AS source_schema,
            tc.table_name AS source_table,
            kcu.column_name AS source_column,
            ccu.table_schema AS target_schema,
            ccu.table_name AS target_table,
            ccu.column_name AS target_column,
            rc.match_option,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
        foreign_keys = [
            ForeignKeyInfo(
                constraint_name=row['constraint_name'],
                source_schema=row['source_schema'],
                source_table=row['source_table'],
                source_column=row['source_column'],
                target_schema=row['target_schema'],
                target_table=row['target_table'],
                target_column=row['target_column'],
                match_option=row['match_option'],
                update_rule=row['update_rule'],
                delete_rule=row['delete_rule']
            )
            for row in results
        ]
        
        logger.info(f"Extracted {len(foreign_keys)} foreign key relationships")
        return foreign_keys
    
    def extract_constraints(self) -> List[ConstraintInfo]:
        """Extract constraint information."""
        query = """
        WITH constraint_columns AS (
            SELECT 
                tc.constraint_name,
                tc.table_schema,
                tc.table_name,
                tc.constraint_type,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as column_names,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name 
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.table_schema = cc.constraint_schema
            WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            GROUP BY tc.constraint_name, tc.table_schema, tc.table_name, 
                     tc.constraint_type, cc.check_clause
        )
        SELECT * FROM constraint_columns
        ORDER BY table_schema, table_name, constraint_name;
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
        constraints = [
            ConstraintInfo(
                constraint_name=row['constraint_name'],
                schema_name=row['table_schema'],
                table_name=row['table_name'],
                constraint_type=row['constraint_type'],
                column_names=row['column_names'] or [],
                check_clause=row['check_clause']
            )
            for row in results
        ]
        
        logger.info(f"Extracted {len(constraints)} constraints")
        return constraints
    
    def extract_indexes(self) -> List[IndexInfo]:
        """Extract index information."""
        query = """
        SELECT 
            schemaname as schema_name,
            tablename as table_name,
            indexname as index_name,
            indexdef as index_definition
        FROM pg_indexes 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schemaname, tablename, indexname;
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        
        # Get detailed index information
        detailed_query = """
        SELECT 
            n.nspname as schema_name,
            t.relname as table_name,
            i.relname as index_name,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary,
            array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as column_names,
            am.amname as index_type
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        GROUP BY n.nspname, t.relname, i.relname, ix.indisunique, 
                 ix.indisprimary, am.amname
        ORDER BY n.nspname, t.relname, i.relname;
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(detailed_query)
            detailed_results = cursor.fetchall()
            
        indexes = [
            IndexInfo(
                schema_name=row['schema_name'],
                table_name=row['table_name'],
                index_name=row['index_name'],
                is_unique=row['is_unique'],
                is_primary=row['is_primary'],
                column_names=row['column_names'],
                index_type=row['index_type']
            )
            for row in detailed_results
        ]
        
        logger.info(f"Extracted {len(indexes)} indexes")
        return indexes


class Neo4jKnowledgeGraphBuilder:
    """Builds knowledge graph in Neo4j from database schema."""
    
    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        
    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()
            logger.info("Disconnected from Neo4j database")
    
    def clear_graph(self):
        """Clear existing schema graph."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Cleared existing knowledge graph")
    
    def create_constraints(self):
        """Create uniqueness constraints for nodes."""
        constraints = [
            "CREATE CONSTRAINT table_unique IF NOT EXISTS FOR (t:Table) REQUIRE (t.schema_name, t.table_name) IS UNIQUE",
            "CREATE CONSTRAINT column_unique IF NOT EXISTS FOR (c:Column) REQUIRE (c.schema_name, c.table_name, c.column_name) IS UNIQUE",
            "CREATE CONSTRAINT constraint_unique IF NOT EXISTS FOR (con:Constraint) REQUIRE (con.schema_name, con.table_name, con.constraint_name) IS UNIQUE",
            "CREATE CONSTRAINT index_unique IF NOT EXISTS FOR (i:Index) REQUIRE (i.schema_name, i.table_name, i.index_name) IS UNIQUE"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    logger.warning(f"Constraint creation warning: {e}")
        
        logger.info("Created Neo4j constraints")
    
    def load_tables(self, tables: List[TableInfo]):
        """Load table nodes into Neo4j."""
        query = """
        UNWIND $tables AS table
        CREATE (t:Table {
            schema_name: table.schema_name,
            table_name: table.table_name,
            table_type: table.table_type,
            comment: table.comment,
            full_name: table.schema_name + '.' + table.table_name
        })
        """
        
        table_data = [
            {
                'schema_name': table.schema_name,
                'table_name': table.table_name,
                'table_type': table.table_type,
                'comment': table.comment
            }
            for table in tables
        ]
        
        with self.driver.session() as session:
            session.run(query, tables=table_data)
        
        logger.info(f"Loaded {len(tables)} table nodes")
    
    def load_columns(self, columns: List[ColumnInfo]):
        """Load column nodes and relationships to tables."""
        query = """
        UNWIND $columns AS col
        MATCH (t:Table {schema_name: col.schema_name, table_name: col.table_name})
        CREATE (c:Column {
            schema_name: col.schema_name,
            table_name: col.table_name,
            column_name: col.column_name,
            data_type: col.data_type,
            is_nullable: col.is_nullable,
            column_default: col.column_default,
            ordinal_position: col.ordinal_position,
            character_maximum_length: col.character_maximum_length,
            numeric_precision: col.numeric_precision,
            numeric_scale: col.numeric_scale,
            comment: col.comment,
            full_name: col.schema_name + '.' + col.table_name + '.' + col.column_name
        })
        CREATE (t)-[:HAS_COLUMN]->(c)
        """
        
        column_data = [
            {
                'schema_name': col.schema_name,
                'table_name': col.table_name,
                'column_name': col.column_name,
                'data_type': col.data_type,
                'is_nullable': col.is_nullable,
                'column_default': col.column_default,
                'ordinal_position': col.ordinal_position,
                'character_maximum_length': col.character_maximum_length,
                'numeric_precision': col.numeric_precision,
                'numeric_scale': col.numeric_scale,
                'comment': col.comment
            }
            for col in columns
        ]
        
        with self.driver.session() as session:
            session.run(query, columns=column_data)
        
        logger.info(f"Loaded {len(columns)} column nodes")
    
    def load_foreign_keys(self, foreign_keys: List[ForeignKeyInfo]):
        """Load foreign key relationships."""
        query = """
        UNWIND $foreign_keys AS fk
        MATCH (source_col:Column {
            schema_name: fk.source_schema, 
            table_name: fk.source_table, 
            column_name: fk.source_column
        })
        MATCH (target_col:Column {
            schema_name: fk.target_schema, 
            table_name: fk.target_table, 
            column_name: fk.target_column
        })
        CREATE (source_col)-[:REFERENCES {
            constraint_name: fk.constraint_name,
            match_option: fk.match_option,
            update_rule: fk.update_rule,
            delete_rule: fk.delete_rule
        }]->(target_col)
        """
        
        fk_data = [
            {
                'constraint_name': fk.constraint_name,
                'source_schema': fk.source_schema,
                'source_table': fk.source_table,
                'source_column': fk.source_column,
                'target_schema': fk.target_schema,
                'target_table': fk.target_table,
                'target_column': fk.target_column,
                'match_option': fk.match_option,
                'update_rule': fk.update_rule,
                'delete_rule': fk.delete_rule
            }
            for fk in foreign_keys
        ]
        
        with self.driver.session() as session:
            session.run(query, foreign_keys=fk_data)
        
        logger.info(f"Loaded {len(foreign_keys)} foreign key relationships")
    
    def load_constraints(self, constraints: List[ConstraintInfo]):
        """Load constraint nodes and relationships."""
        query = """
        UNWIND $constraints AS constraint
        MATCH (t:Table {schema_name: constraint.schema_name, table_name: constraint.table_name})
        CREATE (con:Constraint {
            constraint_name: constraint.constraint_name,
            schema_name: constraint.schema_name,
            table_name: constraint.table_name,
            constraint_type: constraint.constraint_type,
            column_names: constraint.column_names,
            check_clause: constraint.check_clause
        })
        CREATE (t)-[:HAS_CONSTRAINT]->(con)
        
        WITH con, constraint
        UNWIND constraint.column_names AS col_name
        MATCH (c:Column {
            schema_name: constraint.schema_name, 
            table_name: constraint.table_name, 
            column_name: col_name
        })
        CREATE (con)-[:APPLIES_TO]->(c)
        """
        
        constraint_data = [
            {
                'constraint_name': con.constraint_name,
                'schema_name': con.schema_name,
                'table_name': con.table_name,
                'constraint_type': con.constraint_type,
                'column_names': con.column_names,
                'check_clause': con.check_clause
            }
            for con in constraints
        ]
        
        with self.driver.session() as session:
            session.run(query, constraints=constraint_data)
        
        logger.info(f"Loaded {len(constraints)} constraint nodes")
    
    def load_indexes(self, indexes: List[IndexInfo]):
        """Load index nodes and relationships."""
        query = """
        UNWIND $indexes AS idx
        MATCH (t:Table {schema_name: idx.schema_name, table_name: idx.table_name})
        CREATE (i:Index {
            schema_name: idx.schema_name,
            table_name: idx.table_name,
            index_name: idx.index_name,
            is_unique: idx.is_unique,
            is_primary: idx.is_primary,
            column_names: idx.column_names,
            index_type: idx.index_type
        })
        CREATE (t)-[:HAS_INDEX]->(i)
        
        WITH i, idx
        UNWIND idx.column_names AS col_name
        MATCH (c:Column {
            schema_name: idx.schema_name, 
            table_name: idx.table_name, 
            column_name: col_name
        })
        CREATE (i)-[:INDEXES]->(c)
        """
        
        index_data = [
            {
                'schema_name': idx.schema_name,
                'table_name': idx.table_name,
                'index_name': idx.index_name,
                'is_unique': idx.is_unique,
                'is_primary': idx.is_primary,
                'column_names': idx.column_names,
                'index_type': idx.index_type
            }
            for idx in indexes
        ]
        
        with self.driver.session() as session:
            session.run(query, indexes=index_data)
        
        logger.info(f"Loaded {len(indexes)} index nodes")


def main():
    """Main execution function."""
    # Database connection parameters
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'postgres')
    }
    
    # Neo4j connection parameters
    neo4j_host = os.getenv('NEO4J_HOST', 'localhost')
    neo4j_port = os.getenv('NEO4J_PORT', '7687')
    neo4j_uri = f"bolt://{neo4j_host}:{neo4j_port}"
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'neo4j')
    
    logger.info("Starting database schema to Neo4j knowledge graph conversion")
    
    # Extract schema from PostgreSQL
    extractor = DatabaseSchemaExtractor(db_params)
    kg_builder = None
    
    try:
        # Connect to databases
        extractor.connect()
        kg_builder = Neo4jKnowledgeGraphBuilder(neo4j_uri, neo4j_user, neo4j_password)
        
        # Extract schema information
        logger.info("Extracting schema information from PostgreSQL...")
        tables = extractor.extract_tables()
        columns = extractor.extract_columns()
        foreign_keys = extractor.extract_foreign_keys()
        constraints = extractor.extract_constraints()
        indexes = extractor.extract_indexes()
        
        # Build knowledge graph in Neo4j
        logger.info("Building knowledge graph in Neo4j...")
        kg_builder.clear_graph()
        kg_builder.create_constraints()
        
        kg_builder.load_tables(tables)
        kg_builder.load_columns(columns)
        kg_builder.load_foreign_keys(foreign_keys)
        kg_builder.load_constraints(constraints)
        kg_builder.load_indexes(indexes)
        
        logger.info("Schema knowledge graph successfully created in Neo4j!")
        
        # Print summary
        print("\n" + "="*60)
        print("SCHEMA KNOWLEDGE GRAPH SUMMARY")
        print("="*60)
        print(f"Tables loaded: {len(tables)}")
        print(f"Columns loaded: {len(columns)}")
        print(f"Foreign keys loaded: {len(foreign_keys)}")
        print(f"Constraints loaded: {len(constraints)}")
        print(f"Indexes loaded: {len(indexes)}")
        print("\nYou can now query the schema knowledge graph in Neo4j!")
        print("Example queries:")
        print("- MATCH (t:Table) RETURN t.table_name, t.schema_name")
        print("- MATCH (t:Table)-[:HAS_COLUMN]->(c:Column) RETURN t.table_name, c.column_name, c.data_type")
        print("- MATCH (c1:Column)-[:REFERENCES]->(c2:Column) RETURN c1.full_name, c2.full_name")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)
    
    finally:
        # Clean up connections
        if extractor:
            extractor.disconnect()
        if kg_builder:
            kg_builder.close()


if __name__ == "__main__":
    main()
