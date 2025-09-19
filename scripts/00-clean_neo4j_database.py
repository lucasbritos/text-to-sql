#!/usr/bin/env python3
"""
Neo4j Database Cleaner

This script deletes all data from the Neo4j database.
"""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()


def main():
    """Delete all data from Neo4j database."""
    # Neo4j connection parameters
    neo4j_host = os.getenv('NEO4J_HOST', 'localhost')
    neo4j_port = os.getenv('NEO4J_PORT', '7687')
    neo4j_uri = f"bolt://{neo4j_host}:{neo4j_port}"
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'neo4j')
    
    try:
        # Connect to Neo4j
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        with driver.session() as session:
            # Delete all nodes and relationships
            session.run("MATCH (n) DETACH DELETE n")
            print("✅ All data deleted from Neo4j database")
        
        driver.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
