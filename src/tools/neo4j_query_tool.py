"""
Neo4j Query Tool for Text-to-SQL MCP Server
Allows agents to execute raw Cypher queries for PostgreSQL schema exploration and SQL generation.
"""

import os
import logging
from typing import Dict, Any, List
from neo4j import GraphDatabase
from dotenv import load_dotenv
import re

from mcp_server import MCPTool

load_dotenv()
logger = logging.getLogger(__name__)


class Neo4jQueryTool(MCPTool):
    """Tool for executing raw Cypher queries to explore PostgreSQL schema (stored in Neo4j) for SQL generation.
    
    This tool allows AI agents to execute custom Cypher queries against the PostgreSQL schema
    stored in Neo4j. Use this to explore table relationships, discover foreign keys, analyze
    schema patterns, and gather information needed to generate accurate PostgreSQL SQL queries.
    
    IMPORTANT: This explores PostgreSQL database schema, not Neo4j application data.
    Use the results to understand PostgreSQL table structure and generate PostgreSQL SQL queries.
    """
    
    def __init__(self):
        super().__init__(
            name="neo4j_query",
            description="Execute raw Cypher queries to explore PostgreSQL schema (stored in Neo4j) for PostgreSQL SQL generation. Query PostgreSQL tables, columns, constraints, and relationships to understand database structure and generate accurate SQL queries. READ-ONLY queries only.",
            parameters=[
                {
                    'name': 'cypher_query',
                    'type': 'string',
                    'description': 'Cypher query to explore PostgreSQL schema. Use to find tables, columns, relationships, constraints. Examples: MATCH (t:Table) RETURN t.table_name; MATCH (t:Table)-[:HAS_COLUMN]->(c:Column) WHERE t.table_name = "customer" RETURN c.column_name, c.data_type',
                    'required': True
                },
                {
                    'name': 'limit',
                    'type': 'integer',
                    'description': 'Maximum number of results to return (default: 100, max: 1000)',
                    'required': False
                },
                {
                    'name': 'include_query_plan',
                    'type': 'boolean',
                    'description': 'Include query execution plan for optimization (default: false)',
                    'required': False
                }
            ]
        )
        
        # Initialize Neo4j connection
        self.neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
        self.neo4j_password = os.getenv('NEO4J_PASSWORD', 'neo4j')
        
        self.driver = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Neo4j."""
        try:
            self.driver = GraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_user, self.neo4j_password)
            )
            # Test connection
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info("Neo4j query tool connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            logger.error("Make sure Neo4j is running: docker-compose up -d neo4j")
            self.driver = None
    
    def _execute_query(self, query: str, limit: int = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results."""
        if not self.driver:
            raise RuntimeError("Not connected to Neo4j database. Check if Neo4j is running and credentials are correct.")
        
        # Apply limit if specified and not already in query
        if limit and "LIMIT" not in query.upper():
            query = f"{query.rstrip(';')} LIMIT {limit}"
        
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [record.data() for record in result]
        except Exception as e:
            logger.error(f"Neo4j query error: {str(e)}")
            logger.error(f"Failed query: {query[:200]}...")
            raise RuntimeError(f"Query execution failed: {str(e)}")
    
    def _validate_query(self, query: str) -> None:
        """Validate that query is read-only and safe."""
        # Remove comments and normalize whitespace
        clean_query = re.sub(r'//.*?\n|/\*.*?\*/', '', query, flags=re.DOTALL)
        clean_query = ' '.join(clean_query.split()).upper()
        
        # Check for dangerous keywords
        dangerous_keywords = [
            'CREATE', 'DELETE', 'DETACH DELETE', 'REMOVE', 'SET', 'MERGE',
            'DROP', 'ALTER', 'CALL', 'LOAD CSV', 'IMPORT', 'EXPORT',
            'ADMIN', 'DBMS', 'TERMINATE', 'KILL'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in clean_query:
                raise ValueError(f"Query contains dangerous keyword: {keyword}. Only read-only queries are allowed.")
        
        # Must start with MATCH, RETURN, SHOW, or EXPLAIN
        allowed_starts = ['MATCH', 'RETURN', 'SHOW', 'EXPLAIN', 'PROFILE', 'WITH', 'UNWIND', 'OPTIONAL MATCH']
        if not any(clean_query.startswith(start) for start in allowed_starts):
            raise ValueError("Query must start with a read-only operation (MATCH, RETURN, SHOW, etc.)")
    
    def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the Cypher query tool."""
        cypher_query = params.get('cypher_query', '').strip()
        limit = params.get('limit', 100)
        include_query_plan = params.get('include_query_plan', False)
        
        if not cypher_query:
            raise ValueError("cypher_query parameter is required")
        
        # Validate limit
        if limit and (not isinstance(limit, int) or limit <= 0 or limit > 1000):
            raise ValueError("limit must be a positive integer between 1 and 1000")
        
        try:
            # Validate query is safe
            self._validate_query(cypher_query)
            
            # Execute query
            results = self._execute_query(cypher_query, limit)
            
            response = {
                'database_type': 'PostgreSQL',
                'storage_type': 'Neo4j',
                'purpose': 'PostgreSQL schema exploration for SQL generation',
                'query': cypher_query,
                'result_count': len(results),
                'results': results,
                'guidance': {
                    'use_case': 'Explore PostgreSQL schema to generate SQL queries',
                    'entity_types': 'Table, Column, Constraint, Index',
                    'relationships': 'HAS_COLUMN, REFERENCES (FK), HAS_CONSTRAINT, HAS_INDEX, INDEXES',
                    'sql_generation_tip': 'Use table_name and column info to build SELECT, JOIN, WHERE clauses'
                }
            }
            
            # Add query plan if requested
            if include_query_plan:
                try:
                    plan_query = f"EXPLAIN {cypher_query}"
                    plan_results = self._execute_query(plan_query)
                    response['query_plan'] = plan_results
                except Exception as e:
                    logger.warning(f"Could not get query plan: {str(e)}")
                    response['query_plan_error'] = str(e)
            
            return response
            
        except Exception as e:
            logger.error(f"Cypher query execution error: {str(e)}")
            raise ValueError(f"Query execution failed: {str(e)}")
    
    def __del__(self):
        """Clean up Neo4j connection."""
        if self.driver:
            try:
                self.driver.close()
            except:
                pass
