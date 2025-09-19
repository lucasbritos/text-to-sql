# Database Schema to Neo4j Knowledge Graph Parser

This script extracts database schema information from PostgreSQL and creates a comprehensive knowledge graph in Neo4j. The knowledge graph represents tables, columns, foreign keys, constraints, and indexes as nodes with meaningful relationships.

## Features

### Extracted Schema Elements
- **Tables**: Basic table information including schema, name, type, and comments
- **Columns**: Detailed column metadata including data types, nullability, defaults, and constraints
- **Foreign Keys**: Complete foreign key relationships with referential actions
- **Constraints**: All constraint types (PRIMARY KEY, UNIQUE, CHECK, etc.)
- **Indexes**: Index information including uniqueness, type, and indexed columns

### Neo4j Knowledge Graph Structure

#### Node Types
- `Table`: Represents database tables
- `Column`: Represents table columns
- `Constraint`: Represents table constraints
- `Index`: Represents table indexes

#### Relationships
- `(Table)-[:HAS_COLUMN]->(Column)`: Table contains columns
- `(Table)-[:HAS_CONSTRAINT]->(Constraint)`: Table has constraints
- `(Table)-[:HAS_INDEX]->(Index)`: Table has indexes
- `(Column)-[:REFERENCES]->(Column)`: Foreign key relationships
- `(Constraint)-[:APPLIES_TO]->(Column)`: Constraint applies to columns
- `(Index)-[:INDEXES]->(Column)`: Index covers specific columns

## Setup and Usage

### 1. Install Dependencies
```bash
# Create virtual environment (if not already created)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### 2. Setup Environment Variables
```bash
# Copy environment template
cp env.example .env

# Edit .env file with your database credentials
```

### 3. Start Docker Services
```bash
# Start PostgreSQL and Neo4j
docker-compose up -d

# Wait for services to be ready (especially Neo4j takes a moment)
```

### 4. Set Neo4j Password (First Time Only)
1. Open Neo4j Browser: http://localhost:7474
2. Login with default credentials: `neo4j/neo4j`
3. Set a new password (update your `.env` file accordingly)

### 5. Run the Schema Parser
```bash
# Make script executable
chmod +x scripts/00-parse_schema.py

# Run the parser
python scripts/00-parse_schema.py
```

## Example Neo4j Queries

After running the parser, you can explore the schema knowledge graph with these example queries:

### Basic Exploration
```cypher
// List all tables
MATCH (t:Table) 
RETURN t.schema_name, t.table_name, t.table_type

// Show table with column count
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN t.table_name, count(c) as column_count
ORDER BY column_count DESC

// Find all foreign key relationships
MATCH (c1:Column)-[:REFERENCES]->(c2:Column)
RETURN c1.full_name as source, c2.full_name as target
```

### Schema Analysis Queries
```cypher
// Find tables with the most foreign keys
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:REFERENCES]->()
RETURN t.table_name, count(c) as fk_count
ORDER BY fk_count DESC

// Find tables that are most referenced (popular targets)
MATCH (c1:Column)-[:REFERENCES]->(c2:Column)<-[:HAS_COLUMN]-(t:Table)
RETURN t.table_name, count(c1) as references_count
ORDER BY references_count DESC

// Show table relationship graph
MATCH (t1:Table)-[:HAS_COLUMN]->(c1:Column)-[:REFERENCES]->(c2:Column)<-[:HAS_COLUMN]-(t2:Table)
RETURN DISTINCT t1.table_name as from_table, t2.table_name as to_table

// Find columns with indexes
MATCH (i:Index)-[:INDEXES]->(c:Column)
RETURN c.table_name, c.column_name, i.index_name, i.is_unique, i.is_primary

// Find check constraints
MATCH (con:Constraint)
WHERE con.constraint_type = 'CHECK'
RETURN con.table_name, con.constraint_name, con.check_clause
```

### Query Generation Assistance
```cypher
// Find related tables for JOIN operations
MATCH path = (t1:Table)-[:HAS_COLUMN]->(:Column)-[:REFERENCES]->(:Column)<-[:HAS_COLUMN]-(t2:Table)
WHERE t1.table_name = 'customer'  // Replace with your table of interest
RETURN DISTINCT t2.table_name as related_table, 
       length(path) as relationship_distance

// Find all paths between two tables
MATCH path = (t1:Table)-[:HAS_COLUMN*..10]-(t2:Table)
WHERE t1.table_name = 'customer' AND t2.table_name = 'payment'
RETURN path
LIMIT 5
```

## Use Cases for Query Generation

This knowledge graph enables several query generation scenarios:

1. **Automatic JOIN Discovery**: Find how tables are related through foreign keys
2. **Schema Understanding**: Explore table relationships and constraints
3. **Query Optimization**: Identify available indexes for performance
4. **Data Validation**: Understand constraints and data types
5. **Schema Documentation**: Generate automatic documentation from the graph

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure Docker services are running and ports are available
2. **Neo4j Authentication**: Make sure Neo4j password is set correctly in `.env`
3. **Missing Data**: Check PostgreSQL connection and verify database has schema
4. **Permission Issues**: Ensure script has execution permissions

### Checking Services
```bash
# Check if services are running
docker-compose ps

# View logs if needed
docker-compose logs postgres
docker-compose logs neo4j
```

### Resetting the Knowledge Graph
The script automatically clears the existing graph before loading new data. To manually clear:
```cypher
MATCH (n) DETACH DELETE n
```

## Extending the Parser

The script is designed to be extensible. You can:

1. Add new schema elements (views, functions, triggers)
2. Enhance relationship modeling
3. Add metadata enrichment
4. Implement incremental updates
5. Add support for other database systems

## Performance Considerations

- For large schemas, consider batch processing
- Neo4j constraints improve query performance
- Index node properties used in frequent queries
- Monitor memory usage for very large schemas
