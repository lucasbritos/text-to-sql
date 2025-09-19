"""
Neo4j Schema Tool for Text-to-SQL MCP Server
Exposes Neo4j database schema information for SQL generation.
"""

import os
import logging
from typing import Dict, Any, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

from mcp_server import MCPTool

load_dotenv()
logger = logging.getLogger(__name__)


class Neo4jSchemaTool(MCPTool):
    """Tool for exploring PostgreSQL database schema (stored in Neo4j) for PostgreSQL SQL query generation.
    
    This tool provides schema information from the Pagila PostgreSQL database that has been
    parsed and stored in Neo4j as a knowledge graph. Use this information to generate
    accurate PostgreSQL SQL queries by understanding table structures, column types,
    relationships, constraints, and indexes.
    """
    
    def __init__(self):
        super().__init__(
            name="neo4j_schema",
            description="Get comprehensive PostgreSQL database schema information (stored in Neo4j) for PostgreSQL SQL query generation. This tool exposes PostgreSQL tables, columns, constraints, and relationships from the Pagila database to help generate accurate PostgreSQL SQL queries.",
            parameters=[
                {
                    'name': 'schema_type',
                    'type': 'string',
                    'description': 'Type of PostgreSQL schema information to retrieve for SQL generation',
                    'required': True,
                    'enum': ['overview', 'nodes', 'relationships', 'properties', 'sample_data']
                },
                {
                    'name': 'node_label',
                    'type': 'string',
                    'description': 'Specific PostgreSQL entity type to analyze: Table, Column, Constraint, or Index',
                    'required': False
                },
                {
                    'name': 'limit',
                    'type': 'integer',
                    'description': 'Maximum number of results for sample PostgreSQL schema data',
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
            # Test connection and check APOC availability
            with self.driver.session() as session:
                session.run("RETURN 1")
                # Check if APOC is available
                try:
                    session.run("RETURN apoc.version()")
                    logger.info("Connected to Neo4j database with APOC plugin")
                except:
                    logger.info("Connected to Neo4j database (APOC not available)")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            logger.error("Make sure Neo4j is running: docker-compose up -d neo4j")
            self.driver = None
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results."""
        if not self.driver:
            raise RuntimeError("Not connected to Neo4j database. Check if Neo4j is running and credentials are correct.")
        
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [record.data() for record in result]
        except Exception as e:
            logger.error(f"Neo4j query error: {str(e)}")
            logger.error(f"Failed query: {query[:100]}...")
            raise RuntimeError(f"Query execution failed: {str(e)}")
    
    def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the schema exploration tool."""
        schema_type = params.get('schema_type')
        node_label = params.get('node_label')
        limit = params.get('limit', 100)
        
        if not schema_type:
            raise ValueError("schema_type parameter is required")
        
        try:
            if schema_type == 'overview':
                return self._get_database_overview()
            elif schema_type == 'nodes':
                return self._get_node_labels()
            elif schema_type == 'relationships':
                return self._get_relationship_types()
            elif schema_type == 'properties':
                return self._get_node_properties(node_label)
            elif schema_type == 'sample_data':
                return self._get_sample_data(limit)
            else:
                raise ValueError(f"Unknown schema_type: {schema_type}")
                
        except Exception as e:
            logger.error(f"Schema exploration error: {str(e)}")
            raise ValueError(f"Schema exploration failed: {str(e)}")
    
    def _get_database_overview(self) -> Dict[str, Any]:
        """Get overall database statistics."""
        queries = {
            'node_count': "MATCH (n) RETURN count(n) as count",
            'relationship_count': "MATCH ()-[r]->() RETURN count(r) as count",
            'labels': "CALL db.labels() YIELD label RETURN collect(label) as labels",
            'relationship_types': "CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) as types"
        }
        
        results = {}
        for key, query in queries.items():
            try:
                result = self._execute_query(query)
                if key in ['node_count', 'relationship_count']:
                    results[key] = result[0]['count'] if result else 0
                else:
                    results[key] = result[0][key.split('_')[0]] if result else []
            except Exception as e:
                logger.warning(f"Failed to get {key}: {str(e)}")
                results[key] = None
        
        return {
            'database_type': 'PostgreSQL',
            'storage_type': 'Neo4j',
            'source_database': 'Pagila PostgreSQL Database',
            'schema_type': 'overview',
            'purpose': 'PostgreSQL SQL query generation',
            'overview': {
                'total_postgresql_entities': results['node_count'],
                'total_relationships': results['relationship_count'],
                'postgresql_entity_types': results['labels'],  # Table, Column, Constraint, Index
                'relationship_types': results['relationship_types'],
                'entity_type_count': len(results['labels']) if results['labels'] else 0,
                'relationship_type_count': len(results['relationship_types']) if results['relationship_types'] else 0
            }
        }
    
    def _get_node_labels(self) -> Dict[str, Any]:
        """Get all node labels with counts."""
        # Try APOC first for accurate counts
        apoc_query = """
        CALL db.labels() YIELD label
        CALL {
            WITH label
            CALL apoc.cypher.run('MATCH (n:' + label + ') RETURN count(n) as count', {}) 
            YIELD value
            RETURN value.count as count
        }
        RETURN label, count
        ORDER BY count DESC
        """
        
        # Standard Cypher fallback (slower but works without APOC)
        fallback_query = """
        CALL db.labels() YIELD label
        CALL {
            WITH label
            MATCH (n) WHERE label IN labels(n)
            RETURN count(n) as count
        }
        RETURN label, count
        ORDER BY count DESC
        """
        
        # Simple fallback if both fail
        simple_query = """
        CALL db.labels() YIELD label
        RETURN label, 0 as count
        ORDER BY label
        """
        
        try:
            results = self._execute_query(apoc_query)
            logger.info("Using APOC for node counts")
        except Exception as e:
            logger.info(f"APOC not available ({str(e)}), trying standard Cypher")
            try:
                results = self._execute_query(fallback_query)
                logger.info("Using standard Cypher for node counts")
            except Exception as e2:
                logger.warning(f"Standard Cypher failed ({str(e2)}), using simple query")
                results = self._execute_query(simple_query)
        
        return {
            'database_type': 'PostgreSQL',
            'storage_type': 'Neo4j', 
            'schema_type': 'nodes',
            'purpose': 'Identify PostgreSQL entities for SQL generation',
            'postgresql_entities': results  # Table, Column, Constraint, Index counts
        }
    
    def _get_relationship_types(self) -> Dict[str, Any]:
        """Get all relationship types with counts."""
        # Try APOC first for accurate counts
        apoc_query = """
        CALL db.relationshipTypes() YIELD relationshipType
        CALL {
            WITH relationshipType
            CALL apoc.cypher.run('MATCH ()-[r:' + relationshipType + ']->() RETURN count(r) as count', {}) 
            YIELD value
            RETURN value.count as count
        }
        RETURN relationshipType, count
        ORDER BY count DESC
        """
        
        # Standard Cypher fallback (slower but works without APOC)
        fallback_query = """
        CALL db.relationshipTypes() YIELD relationshipType
        CALL {
            WITH relationshipType
            MATCH ()-[r]->() WHERE type(r) = relationshipType
            RETURN count(r) as count
        }
        RETURN relationshipType, count
        ORDER BY count DESC
        """
        
        # Simple fallback if both fail
        simple_query = """
        CALL db.relationshipTypes() YIELD relationshipType
        RETURN relationshipType, 0 as count
        ORDER BY relationshipType
        """
        
        try:
            results = self._execute_query(apoc_query)
            logger.info("Using APOC for relationship counts")
        except Exception as e:
            logger.info(f"APOC not available ({str(e)}), trying standard Cypher")
            try:
                results = self._execute_query(fallback_query)
                logger.info("Using standard Cypher for relationship counts")
            except Exception as e2:
                logger.warning(f"Standard Cypher failed ({str(e2)}), using simple query")
                results = self._execute_query(simple_query)
        
        return {
            'database_type': 'PostgreSQL',
            'storage_type': 'Neo4j',
            'schema_type': 'relationships', 
            'purpose': 'Understand PostgreSQL relationships for JOIN operations',
            'postgresql_relationships': results  # HAS_COLUMN, REFERENCES (FK), HAS_CONSTRAINT, etc.
        }
    
    def _get_node_properties(self, node_label: str = None) -> Dict[str, Any]:
        """Get properties for nodes."""
        if node_label:
            # Get properties for specific label
            query = f"""
            MATCH (n:{node_label})
            WITH keys(n) as props
            UNWIND props as prop
            RETURN DISTINCT prop as property
            ORDER BY prop
            LIMIT 100
            """
            results = self._execute_query(query)
            properties = [r['property'] for r in results]
            
            return {
                'database_type': 'PostgreSQL',
                'storage_type': 'Neo4j',
                'schema_type': 'properties',
                'purpose': f'PostgreSQL {node_label.lower()} properties for SQL generation',
                'postgresql_entity_type': node_label,
                'properties': properties
            }
        else:
            # Get properties for all labels
            apoc_query = """
            CALL db.labels() YIELD label
            CALL {
                WITH label
                CALL apoc.cypher.run('MATCH (n:' + label + ') WITH keys(n) as props UNWIND props as prop RETURN DISTINCT prop LIMIT 50', {}) 
                YIELD value
                RETURN value.prop as property
            }
            RETURN label, collect(property) as properties
            ORDER BY label
            """
            
            # Standard Cypher fallback
            fallback_query = """
            CALL db.labels() YIELD label
            CALL {
                WITH label
                MATCH (n) WHERE label IN labels(n)
                WITH DISTINCT keys(n) as props
                UNWIND props as prop_list
                UNWIND prop_list as prop
                RETURN DISTINCT prop
                LIMIT 50
            }
            RETURN label, collect(prop) as properties
            ORDER BY label
            """
            
            # Simple fallback
            simple_query = """
            CALL db.labels() YIELD label
            RETURN label, [] as properties
            ORDER BY label
            """
            
            try:
                results = self._execute_query(apoc_query)
                logger.info("Using APOC for property analysis")
            except Exception as e:
                logger.info(f"APOC not available ({str(e)}), trying standard Cypher")
                try:
                    results = self._execute_query(fallback_query)
                    logger.info("Using standard Cypher for property analysis")
                except Exception as e2:
                    logger.warning(f"Standard Cypher failed ({str(e2)}), using simple query")
                    results = self._execute_query(simple_query)
            
            return {
                'database_type': 'PostgreSQL',
                'storage_type': 'Neo4j',
                'schema_type': 'properties',
                'purpose': 'PostgreSQL entity properties for SQL generation',
                'postgresql_entity_properties': results  # Properties for Table, Column, Constraint, Index
            }
    
    def _get_sample_data(self, limit: int = 10) -> Dict[str, Any]:
        """Get sample nodes and relationships."""
        queries = {
            'sample_nodes': f"MATCH (n) RETURN labels(n) as labels, keys(n) as properties LIMIT {limit}",
            'sample_relationships': f"MATCH (a)-[r]->(b) RETURN labels(a) as from_labels, type(r) as relationship_type, labels(b) as to_labels LIMIT {limit}"
        }
        
        results = {}
        for key, query in queries.items():
            try:
                results[key] = self._execute_query(query)
            except Exception as e:
                logger.warning(f"Failed to get {key}: {str(e)}")
                results[key] = []
        
        return {
            'database_type': 'PostgreSQL',
            'storage_type': 'Neo4j',
            'schema_type': 'sample_data',
            'purpose': 'Sample PostgreSQL schema elements for understanding structure',
            'sample_postgresql_entities': results['sample_nodes'],  # Table, Column, Constraint, Index samples
            'sample_postgresql_relationships': results['sample_relationships']  # HAS_COLUMN, REFERENCES, etc.
        }
    
    def __del__(self):
        """Clean up Neo4j connection."""
        if self.driver:
            try:
                self.driver.close()
            except:
                pass
